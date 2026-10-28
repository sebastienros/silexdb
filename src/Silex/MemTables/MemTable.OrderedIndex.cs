using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Silex.MemTables;

internal sealed partial class MemTable
{
    /// <summary>
    /// A B+-tree of record positions in key order. Each node is one object that holds, next to its record
    /// positions, an 8-byte window of each key, so a search reads the node's own lines and touches a record only
    /// where two windows tie.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A node's windows start past <see cref="Node.PrefixLength"/> bytes that every key a search routes to it
    /// shares, so keys with a long common prefix (MVCC keys, or application keys such as <c>user:</c>) still
    /// differ in them. A node starts at prefix 0, where a window is the record's prefix word, and takes the common
    /// prefix of its fences, the separators beside it in its ancestors, once two of its windows tie. A node with no
    /// fence on one side lies on an edge of the tree and takes the prefix that every key of the tree shares.
    /// </para>
    /// <para>
    /// A window holds the bytes big-endian and zero-padded, so windows order as their keys do and equal windows
    /// leave the keys to be compared from the arena.
    /// </para>
    /// </remarks>
    internal sealed class OrderedIndex
    {
        internal const int LeafCapacity = 256;
        internal const int BranchCapacity = 32;
        private const int NoFence = -1;
        /// <summary>Deeper than any real tree (a height of 6 already holds billions of records).</summary>
        private const int MaxOptimisticDepth = 16;
        private readonly MemTable _table;
        private Node _root;
        private long _version;
        private Node[] _insertPath = new Node[8];
        private int _lockedFrom;
        private int _lockedTo;

        /// <summary>A record whose first <see cref="_prefixLength"/> bytes every key of the tree shares, or <see cref="NoFence"/> while the tree is empty.</summary>
        private int _prefixRecord = NoFence;
        private int _prefixLength;
        public long Version => _version;
        internal Node Root => _root;

        public OrderedIndex(MemTable table, ReadOnlySpan<int> records)
        {
            _table = table;
            if (records.Length != 0)
            {
                _prefixRecord = records[0];
                _prefixLength = Key(records[0]).CommonPrefixLength(Key(records[^1]));
            }
            var height = 0;
            long capacity = LeafCapacity;
            while (capacity < records.Length)
            {
                capacity *= BranchCapacity + 1;
                height++;
            }
            Leaf? previous = null;
            _root = Build(records, height, capacity, NoFence, NoFence, ref previous);
        }

        private Node Build(ReadOnlySpan<int> records, int height, long capacity, int low, int high, ref Leaf? previous)
        {
            if (height == 0)
            {
                var leaf = new Leaf { Count = records.Length };
                records.CopyTo(leaf.Keys);
                var words = leaf.Words;
                for (var i = 0; i < records.Length; i++) words[i] = Window(records[i], 0);
                GrowPrefixIfTied(leaf, low, high);
                leaf.Previous = previous;
                if (previous is not null) previous.Next = leaf;
                previous = leaf;
                return leaf;
            }
            var node = new Branch();
            var childCapacity = capacity / (BranchCapacity + 1);
            var children = (int)((records.Length + childCapacity - 1) / childCapacity);
            node.Count = children - 1;
            for (var i = 0; i < children; i++)
            {
                var count = (int)Math.Min(childCapacity, records.Length);
                var childLow = i == 0 ? low : records[0];
                var childHigh = i == children - 1 ? high : records[count];
                node.Children[i] = Build(records[..count], height - 1, childCapacity, childLow, childHigh, ref previous);
                if (i != 0)
                {
                    node.Keys[i - 1] = records[0];
                    node.Words[i - 1] = Window(records[0], 0);
                }
                records = records[count..];
            }
            GrowPrefixIfTied(node, low, high);
            return node;
        }

        public Cursor Begin(bool backwards)
        {
            var leaf = Edge(backwards);
            return new Cursor(this, leaf, backwards ? leaf.Count - 1 : 0);
        }

        private Leaf Edge(bool last)
        {
            var node = _root;
            while (!node.IsLeaf)
            {
                var branch = Unsafe.As<Branch>(node);
                node = branch.Children[last ? branch.Count : 0]!;
            }
            return Unsafe.As<Leaf>(node);
        }

        public Cursor Seek(ReadOnlySpan<byte> key, bool inclusive, bool backwards)
        {
            Leaf leaf;
            int slot;
            var equal = false;
            var outside = CompareToTreePrefix(key);
            if (outside != 0)
            {
                // The key lies before or after every key of the tree, whose windows skip a prefix it lacks.
                leaf = Edge(outside > 0);
                slot = outside > 0 ? leaf.Count : 0;
            }
            else
            {
                var node = _root;
                while (!node.IsLeaf)
                {
                    var branch = Unsafe.As<Branch>(node);
                    node = branch.Children[Bound(branch.Words, branch.Keys, branch.Count, key, branch.PrefixLength, true)]!;
                }
                leaf = Unsafe.As<Leaf>(node);
                var prefixLength = leaf.PrefixLength;
                slot = Bound(leaf.Words, leaf.Keys, leaf.Count, key, prefixLength, false);
                equal = slot < leaf.Count && leaf.Words[slot] == Window(key, prefixLength) && EqualsKey(leaf.Keys[slot], key);
            }
            if (backwards)
            {
                if (!equal || !inclusive) slot--;
                if (slot < 0 && leaf.Previous is { } previous) return new Cursor(this, previous, previous.Count - 1);
            }
            else
            {
                if (equal && !inclusive) slot++;
                if (slot == leaf.Count && leaf.Next is { } next) return new Cursor(this, next, 0);
            }
            return new Cursor(this, leaf, slot);
        }

        /// <summary>Compares <paramref name="key"/> with the prefix that every key of the tree shares: 0 when it holds it.</summary>
        private int CompareToTreePrefix(ReadOnlySpan<byte> key)
        {
            if (_prefixLength == 0) return 0;
            var prefix = Key(_prefixRecord)[.._prefixLength];
            return key.StartsWith(prefix) ? 0 : key.SequenceCompareTo(prefix) < 0 ? -1 : 1;
        }

        /// <summary>
        /// Inserts a record. Once the descent reaches the leaf, and before changing anything, it locks from the top
        /// down every node the insert rewrites: the leaf, and each ancestor that receives a separator because the
        /// node below it splits (re-prefixing a node only ever happens to one of those). An optimistic reader that
        /// crosses one of them while it changes restarts.
        /// </summary>
        public void Insert(int record)
        {
            _version++;
            var key = Key(record);
            if (_prefixRecord == NoFence)
            {
                // The tree prefix and the edge nodes that take it change together; optimistic readers validate the
                // table's layout version, which covers both.
                _table.BeginLayoutChange();
                _prefixRecord = record;
                _prefixLength = key.Length;
                _table.EndLayoutChange();
            }
            else if (_prefixLength != 0)
            {
                var shared = key.CommonPrefixLength(Key(_prefixRecord)[.._prefixLength]);
                if (shared < _prefixLength) ShrinkTreePrefix(shared);
            }
            _lockedFrom = _lockedTo = 0;
            try
            {
                var right = Insert(_root, 0, record, key, NoFence, NoFence, out var separator);
                if (right is null) return;
                var root = new Branch { Count = 1 };
                root.Keys[0] = separator;
                root.Words[0] = Window(separator, 0);
                root.Children[0] = _root;
                root.Children[1] = right;
                Volatile.Write(ref _root, root);
            }
            finally
            {
                for (var i = _lockedFrom; i < _lockedTo; i++) Unlock(_insertPath[i]);
                _insertPath.AsSpan(0, _lockedTo).Clear();
            }
        }

        /// <summary>Locks the nodes of <c>_insertPath</c> that the insert into the leaf at <paramref name="depth"/> rewrites.</summary>
        private void LockInsertPath(int depth)
        {
            var top = depth;
            while (top > 0 && WillSplit(_insertPath[top])) top--;
            _lockedFrom = top;
            _lockedTo = depth + 1;
            for (var i = top; i <= depth; i++) Lock(_insertPath[i]);
        }

        private static bool WillSplit(Node node) => node.Count + 1 > (node.IsLeaf ? LeafCapacity : BranchCapacity);

        private static void Lock(Node node) => Interlocked.Increment(ref node.Version);

        private static void Unlock(Node node) => Volatile.Write(ref node.Version, node.Version + 1);

        /// <summary>
        /// Finds the leaf and slot of the first record at or after <paramref name="key"/> without a lock, in the
        /// style of optimistic lock coupling: each child's version is read before its parent is validated, so the
        /// descent only follows pointers that were current. Any node may be read mid-change (count, windows,
        /// record positions, children or prefix), so every read is bounded and a failure returns false. The caller
        /// validates <paramref name="version"/> after reading the leaf, and the table's layout version, which
        /// covers the tree prefix and the edge nodes that take it.
        /// </summary>
        public bool TrySeekOptimistic(ReadOnlySpan<byte> key, MemTableRecord[] records, out Leaf leaf, out int slot, out int version)
        {
            leaf = null!;
            slot = 0;
            version = 0;
            var outside = CompareToTreePrefixOptimistic(key, records);
            if (outside == int.MinValue) return false;
            var node = Volatile.Read(ref _root);
            var nodeVersion = Volatile.Read(ref node.Version);
            if ((nodeVersion & 1) != 0 || !ReferenceEquals(node, Volatile.Read(ref _root))) return false;
            for (var depth = 0; !node.IsLeaf; depth++)
            {
                var branch = Unsafe.As<Branch>(node);
                var children = branch.Children;
                var childSlot = outside == 0
                    ? BoundOptimistic(branch.Words, branch.Keys, branch.Count, key, branch.PrefixLength, true, records)
                    : outside < 0 ? 0 : branch.Count;
                if (depth == MaxOptimisticDepth || (uint)childSlot >= (uint)children.Length) return false;
                var child = Volatile.Read(ref children[childSlot]);
                if (child is null) return false;
                var childVersion = Volatile.Read(ref child.Version);
                ReadBarrier();
                if ((childVersion & 1) != 0 || Volatile.Read(ref node.Version) != nodeVersion) return false;
                node = child;
                nodeVersion = childVersion;
            }
            leaf = Unsafe.As<Leaf>(node);
            slot = outside == 0
                ? BoundOptimistic(leaf.Words, leaf.Keys, leaf.Count, key, leaf.PrefixLength, false, records)
                : outside < 0 ? 0 : leaf.Count;
            if ((uint)slot > LeafCapacity + 1) return false;
            version = nodeVersion;
            ReadBarrier();
            return Volatile.Read(ref leaf.Version) == nodeVersion;
        }

        /// <summary><see cref="CompareToTreePrefix"/> over state that may be torn: <see cref="int.MinValue"/> when it cannot tell.</summary>
        private int CompareToTreePrefixOptimistic(ReadOnlySpan<byte> key, MemTableRecord[] records)
        {
            var prefixLength = _prefixLength;
            if (prefixLength == 0) return 0;
            var prefixRecord = _prefixRecord;
            if ((uint)prefixRecord >= (uint)records.Length ||
                !_table._arena.TryGetSpanOptimistic(records[prefixRecord].Key, out var prefixKey) ||
                (uint)prefixLength > (uint)prefixKey.Length)
            {
                return int.MinValue;
            }
            var prefix = prefixKey[..prefixLength];
            return key.StartsWith(prefix) ? 0 : key.SequenceCompareTo(prefix) < 0 ? -1 : 1;
        }

        /// <summary><see cref="Bound"/> over a node that may be torn: returns -1 instead of reading out of range.</summary>
        private int BoundOptimistic(Span<ulong> words, Span<int> keys, int count, ReadOnlySpan<byte> key, int prefixLength,
            bool upper, MemTableRecord[] records)
        {
            if ((uint)count > (uint)words.Length || (uint)count > (uint)keys.Length || prefixLength < 0) return -1;
            var target = Window(key, prefixLength);
            var low = CountBelow(words, count, target);
            var high = low;
            while (high < count && words[high] == target) high++;
            var arena = _table._arena;
            var end = prefixLength + sizeof(ulong);
            while (low < high)
            {
                var middle = low + ((high - low) >> 1);
                var recordIndex = keys[middle];
                if ((uint)recordIndex >= (uint)records.Length) return -1;
                ref var rec = ref records[recordIndex];
                int comparison;
                if (rec.Key.Length <= end && key.Length <= end)
                {
                    comparison = rec.Key.Length.CompareTo(key.Length);
                }
                else
                {
                    if (!arena.TryGetSpanOptimistic(rec.Key, out var recKey)) return -1;
                    comparison = recKey.SequenceCompareTo(key);
                }
                if (comparison < 0 || upper && comparison == 0) low = middle + 1;
                else high = middle;
            }
            return low;
        }

        /// <summary>
        /// Shortens the tree prefix to <paramref name="length"/> bytes and rewrites the windows of the nodes on the edges, which take it.
        /// Those nodes are not on the insert path, so the table's layout version, odd meanwhile, covers them for optimistic readers.
        /// </summary>
        private void ShrinkTreePrefix(int length)
        {
            _table.BeginLayoutChange();
            try
            {
                ShrinkTreePrefixCore(length);
            }
            finally
            {
                _table.EndLayoutChange();
            }
        }

        private void ShrinkTreePrefixCore(int length)
        {
            _prefixLength = length;
            for (var side = 0; side < 2; side++)
            {
                var node = _root;
                while (true)
                {
                    if (node.PrefixLength > length) SetPrefix(node, length);
                    if (node.IsLeaf) break;
                    var branch = Unsafe.As<Branch>(node);
                    node = branch.Children[side == 0 ? 0 : branch.Count]!;
                }
            }
        }

        private Node? Insert(Node node, int depth, int record, ReadOnlySpan<byte> key, int low, int high, out int separator)
        {
            separator = 0;
            if (depth == _insertPath.Length) Array.Resize(ref _insertPath, depth * 2);
            _insertPath[depth] = node;
            var prefixLength = node.PrefixLength;
            var word = Window(key, prefixLength);
            if (node.IsLeaf)
            {
                var leaf = Unsafe.As<Leaf>(node);
                var words = leaf.Words;
                var keys = leaf.Keys;
                var count = leaf.Count;
                var slot = Bound(words, keys, count, key, prefixLength, false);
                LockInsertPath(depth);
                words[slot..count].CopyTo(words[(slot + 1)..]);
                keys[slot..count].CopyTo(keys[(slot + 1)..]);
                words[slot] = word;
                keys[slot] = record;
                leaf.Count++;
                if (IsTied(words, slot, leaf.Count)) GrowPrefix(leaf, low, high);
                if (leaf.Count <= LeafCapacity) return null;
                _table._faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeOrderedIndexSplit);

                var middle = leaf.Count / 2;
                var right = new Leaf { Count = leaf.Count - middle, PrefixLength = leaf.PrefixLength };
                words[middle..leaf.Count].CopyTo(right.Words);
                keys[middle..leaf.Count].CopyTo(right.Keys);
                separator = keys[middle];
                // The new leaf is complete, prefix included, before a reader can reach it through Next.
                SplitPrefix(right, separator, high);
                right.Next = leaf.Next;
                right.Previous = leaf;
                if (leaf.Next is not null) leaf.Next.Previous = right;
                Volatile.Write(ref leaf.Next, right);
                leaf.Count = middle;
                SplitPrefix(leaf, low, separator);
                return right;
            }
            else
            {
                var branch = Unsafe.As<Branch>(node);
                var words = branch.Words;
                var keys = branch.Keys;
                var children = branch.Children;
                var count = branch.Count;
                var slot = Bound(words, keys, count, key, prefixLength, true);
                var childLow = slot == 0 ? low : keys[slot - 1];
                var childHigh = slot == count ? high : keys[slot];
                var child = Insert(children[slot]!, depth + 1, record, key, childLow, childHigh, out var childSeparator);
                if (child is null) return null;
                words[slot..count].CopyTo(words[(slot + 1)..]);
                keys[slot..count].CopyTo(keys[(slot + 1)..]);
                children[(slot + 1)..(count + 1)].CopyTo(children[(slot + 2)..]);
                words[slot] = Window(childSeparator, prefixLength);
                keys[slot] = childSeparator;
                Volatile.Write(ref children[slot + 1], child);
                branch.Count++;
                if (IsTied(words, slot, branch.Count)) GrowPrefix(branch, low, high);
                if (branch.Count <= BranchCapacity) return null;
                _table._faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeOrderedIndexSplit);

                var middle = branch.Count / 2;
                var right = new Branch { Count = branch.Count - middle - 1, PrefixLength = branch.PrefixLength };
                separator = keys[middle];
                words[(middle + 1)..branch.Count].CopyTo(right.Words);
                keys[(middle + 1)..branch.Count].CopyTo(right.Keys);
                children[(middle + 1)..(branch.Count + 1)].CopyTo(right.Children);
                children[(middle + 1)..(branch.Count + 1)].Clear();
                branch.Count = middle;
                SplitPrefix(branch, low, separator);
                SplitPrefix(right, separator, high);
                return right;
            }
        }

        /// <summary>Gets the prefix length of a node between fences <paramref name="low"/> and <paramref name="high"/>.</summary>
        private int PrefixLength(int low, int high) =>
            low == NoFence || high == NoFence ? _prefixLength : Key(low).CommonPrefixLength(Key(high));

        /// <summary>Whether the window in <paramref name="slot"/> equals a neighbor's, so the two keys must be read to be ordered.</summary>
        private static bool IsTied(Span<ulong> words, int slot, int count) =>
            slot > 0 && words[slot - 1] == words[slot] || slot + 1 < count && words[slot + 1] == words[slot];

        /// <summary>
        /// Lengthens the prefix of <paramref name="node"/> to that of its fences <paramref name="low"/> and <paramref name="high"/>,
        /// so that its windows hold bytes that tell its keys apart.
        /// </summary>
        /// <remarks>
        /// A node keeps a shorter prefix while its windows do not tie: rewriting them reads every key from the arena, and a
        /// window at prefix 0 is the record's own prefix word. Once grown, the node stays at its fences until a split narrows them.
        /// </remarks>
        private void GrowPrefix(Node node, int low, int high)
        {
            if (node.PrefixAtFences) return;
            node.PrefixAtFences = true;
            var prefixLength = PrefixLength(low, high);
            if (prefixLength > node.PrefixLength) SetPrefix(node, prefixLength);
        }

        private void GrowPrefixIfTied(Node node, int low, int high)
        {
            var words = node.IsLeaf ? Unsafe.As<Leaf>(node).Words : Unsafe.As<Branch>(node).Words;
            for (var i = 1; i < node.Count; i++)
            {
                if (words[i - 1] == words[i])
                {
                    GrowPrefix(node, low, high);
                    return;
                }
            }
        }

        /// <summary>Resets the prefix state of a node that a split gave the fences <paramref name="low"/> and <paramref name="high"/>.</summary>
        private void SplitPrefix(Node node, int low, int high)
        {
            node.PrefixAtFences = false;
            GrowPrefixIfTied(node, low, high);
        }

        private void SetPrefix(Node node, int prefixLength)
        {
            node.PrefixLength = prefixLength;
            var words = node.IsLeaf ? Unsafe.As<Leaf>(node).Words : Unsafe.As<Branch>(node).Words;
            var keys = node.IsLeaf ? Unsafe.As<Leaf>(node).Keys : Unsafe.As<Branch>(node).Keys;
            for (var i = 0; i < node.Count; i++) words[i] = Window(keys[i], prefixLength);
        }

        private ReadOnlySpan<byte> Key(int record) => _table._arena.GetSpan(_table._records[record].Key);

        /// <summary>Gets the 8 bytes of the key of <paramref name="record"/> past <paramref name="prefixLength"/>.</summary>
        private ulong Window(int record, int prefixLength) =>
            prefixLength == 0 ? _table._records[record].KeyPrefixWord : Window(Key(record), prefixLength);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Window(ReadOnlySpan<byte> key, int prefixLength) =>
            prefixLength < key.Length ? ReadKeyPrefixWord(key[prefixLength..]) : 0;

        private bool EqualsKey(int record, ReadOnlySpan<byte> key) => Key(record).SequenceEqual(key);

        /// <summary>Compares the key of <paramref name="record"/> with <paramref name="key"/>, whose windows past <paramref name="prefixLength"/> are equal.</summary>
        private int CompareTied(int record, ReadOnlySpan<byte> key, int prefixLength)
        {
            ref var rec = ref _table._records[record];
            var end = prefixLength + sizeof(ulong);
            if (rec.Key.Length <= end && key.Length <= end)
            {
                return rec.Key.Length.CompareTo(key.Length);
            }
            return _table._arena.GetSpan(rec.Key).SequenceCompareTo(key);
        }

        /// <summary>Finds where <paramref name="key"/> goes among the first <paramref name="count"/> keys of a node.</summary>
        /// <param name="upper">Whether a key equal to <paramref name="key"/> goes before it, as branches route equal keys right.</param>
        /// <remarks>
        /// Counts the windows below the key's, then orders the key among the keys whose windows equal its own, which are
        /// next to each other and the only ones read from the arena.
        /// </remarks>
        private int Bound(Span<ulong> words, Span<int> keys, int count, ReadOnlySpan<byte> key, int prefixLength, bool upper)
        {
            var target = Window(key, prefixLength);
            var low = CountBelow(words, count, target);
            var high = low;
            while (high < count && words[high] == target) high++;
            while (low < high)
            {
                var middle = low + ((high - low) >> 1);
                var comparison = CompareTied(keys[middle], key, prefixLength);
                if (comparison < 0 || upper && comparison == 0) low = middle + 1;
                else high = middle;
            }
            return low;
        }

        /// <summary>The windows that a vector count compares once a binary search has narrowed the range to them.</summary>
        private const int CountBlock = 16;

        /// <summary>Counts the windows below <paramref name="target"/> among the first <paramref name="count"/>, which are sorted.</summary>
        /// <remarks>
        /// Where 32-byte vectors are fast (x64) comparing every window measured faster than any binary search; elsewhere
        /// (Arm64) a branch-free binary search down to <see cref="CountBlock"/> windows and a vector count of those measured best.
        /// </remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CountBelow(Span<ulong> words, int count, ulong target)
        {
            ref var first = ref MemoryMarshal.GetReference(words);
            if (Vector256.IsHardwareAccelerated) return CountBelowLinear(ref first, count, target);
            nint start = 0;
            nint length = count;
            while (length > CountBlock)
            {
                var half = length >> 1;
                start = Unsafe.Add(ref first, start + half) < target ? start + half : start;
                length -= half;
            }
            return (int)start + CountBelowLinear(ref Unsafe.Add(ref first, start), (int)length, target);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int CountBelowLinear(ref ulong first, int count, ulong target)
        {
            var i = 0;
            long less = 0;
            if (Vector256.IsHardwareAccelerated)
            {
                var targets = Vector256.Create(target);
                var sum = Vector256<long>.Zero;
                for (; i + Vector256<ulong>.Count <= count; i += Vector256<ulong>.Count)
                    sum -= Vector256.LessThan(Vector256.LoadUnsafe(ref first, (nuint)i), targets).AsInt64();
                less = Vector256.Sum(sum);
            }
            else if (Vector128.IsHardwareAccelerated)
            {
                var targets = Vector128.Create(target);
                var sum = Vector128<long>.Zero;
                for (; i + Vector128<ulong>.Count <= count; i += Vector128<ulong>.Count)
                    sum -= Vector128.LessThan(Vector128.LoadUnsafe(ref first, (nuint)i), targets).AsInt64();
                less = Vector128.Sum(sum);
            }
            for (; i < count; i++) less += Unsafe.Add(ref first, i) < target ? 1 : 0;
            return (int)less;
        }

        public abstract class Node(bool isLeaf)
        {
            public readonly bool IsLeaf = isLeaf;
            public int Count;

            /// <summary>Odd while the writer changes the node, its windows and prefix included; optimistic readers validate against it.</summary>
            public int Version;

            /// <summary>The leading bytes that every key a search routes to the node shares, which its windows skip.</summary>
            public int PrefixLength;

            /// <summary>Whether <see cref="PrefixLength"/> is the common prefix of the node's fences, the longest it can be.</summary>
            public bool PrefixAtFences;
        }

        public sealed class Leaf() : Node(true)
        {
            public Leaf? Previous;
            public Leaf? Next;
            private LeafWords _words;
            private LeafKeys _keys;

            /// <summary>The window of each key, in key order.</summary>
            public Span<ulong> Words => MemoryMarshal.CreateSpan(ref Unsafe.As<LeafWords, ulong>(ref _words), LeafCapacity + 1);

            /// <summary>The record position of each key, in key order.</summary>
            public Span<int> Keys => MemoryMarshal.CreateSpan(ref Unsafe.As<LeafKeys, int>(ref _keys), LeafCapacity + 1);
        }

        public sealed class Branch() : Node(false)
        {
            private BranchWords _words;
            private BranchKeys _keys;
            private BranchChildren _children;

            /// <summary>The window of each separator.</summary>
            public Span<ulong> Words => MemoryMarshal.CreateSpan(ref Unsafe.As<BranchWords, ulong>(ref _words), BranchCapacity + 1);

            /// <summary>The record position of each separator: the least key of the child to its right when it was made.</summary>
            public Span<int> Keys => MemoryMarshal.CreateSpan(ref Unsafe.As<BranchKeys, int>(ref _keys), BranchCapacity + 1);

            public Span<Node?> Children => MemoryMarshal.CreateSpan(ref Unsafe.As<BranchChildren, Node?>(ref _children), BranchCapacity + 2);
        }

        [InlineArray(LeafCapacity + 1)]
        private struct LeafWords { private ulong _element; }

        [InlineArray(LeafCapacity + 1)]
        private struct LeafKeys { private int _element; }

        [InlineArray(BranchCapacity + 1)]
        private struct BranchWords { private ulong _element; }

        [InlineArray(BranchCapacity + 1)]
        private struct BranchKeys { private int _element; }

        [InlineArray(BranchCapacity + 2)]
        private struct BranchChildren { private Node? _element; }

        public struct Cursor
        {
            private OrderedIndex _index;
            private long _version;
            private Leaf _node;
            private int _slot;
            public Cursor(OrderedIndex index, Leaf node, int slot) { _index = index; _version = index._version; _node = node; _slot = slot; }
            public readonly Leaf CurrentNode => _node;
            public readonly int CurrentSlot => _slot;
            public readonly bool IsCurrent(OrderedIndex index) => ReferenceEquals(_index, index) && _version == index._version;
            public readonly bool IsValid => (uint)_slot < (uint)_node.Count;
            public readonly int Record => _node.Keys[_slot];
            public void Move(bool backwards)
            {
                _slot += backwards ? -1 : 1;
                if (_slot == _node.Count && _node.Next is { } next) { _node = next; _slot = 0; }
                else if (_slot < 0 && _node.Previous is { } previous) { _node = previous; _slot = previous.Count - 1; }
            }
        }
    }
}
