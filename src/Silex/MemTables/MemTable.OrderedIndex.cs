using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
    /// A node's window starts past <see cref="Node.PrefixLength"/> bytes that every key a search routes to it
    /// shares, so keys with a long common prefix (MVCC keys, or application keys such as <c>user:</c>) still
    /// differ in it. The prefix of a node is the common prefix of its fences, the separators beside it in its
    /// ancestors; a node with no fence on one side lies on an edge of the tree and takes the prefix that every
    /// key of the tree shares.
    /// </para>
    /// <para>
    /// A window holds the bytes big-endian and zero-padded, so windows order as their keys do and equal windows
    /// leave the keys to be compared from the arena.
    /// </para>
    /// </remarks>
    internal sealed class OrderedIndex : IDisposable
    {
        internal const int LeafCapacity = 256;
        internal const int BranchCapacity = 32;
        private const int NoFence = -1;
        private readonly MemTable _table;
        private Node _root;
        private long _version;

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
            var prefixLength = PrefixLength(low, high);
            if (height == 0)
            {
                var leaf = new Leaf { Count = records.Length, PrefixLength = prefixLength };
                records.CopyTo(leaf.Keys);
                var words = leaf.Words;
                for (var i = 0; i < records.Length; i++) words[i] = Window(records[i], prefixLength);
                leaf.Previous = previous;
                if (previous is not null) previous.Next = leaf;
                previous = leaf;
                return leaf;
            }
            var node = new Branch { PrefixLength = prefixLength };
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
                    node.Words[i - 1] = Window(records[0], prefixLength);
                }
                records = records[count..];
            }
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

        public void Insert(int record)
        {
            _version++;
            var key = Key(record);
            if (_prefixRecord == NoFence)
            {
                _prefixRecord = record;
                _prefixLength = key.Length;
                _root.PrefixLength = key.Length;
            }
            else if (_prefixLength != 0)
            {
                var shared = key.CommonPrefixLength(Key(_prefixRecord)[.._prefixLength]);
                if (shared < _prefixLength) ShrinkTreePrefix(shared);
            }
            var right = Insert(_root, record, key, NoFence, NoFence, out var separator);
            if (right is null) return;
            var root = new Branch { Count = 1, PrefixLength = _prefixLength };
            root.Keys[0] = separator;
            root.Words[0] = Window(separator, _prefixLength);
            root.Children[0] = _root;
            root.Children[1] = right;
            _root = root;
        }

        /// <summary>Shortens the tree prefix to <paramref name="length"/> bytes and rewrites the windows of the nodes on the edges, which take it.</summary>
        private void ShrinkTreePrefix(int length)
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

        private Node? Insert(Node node, int record, ReadOnlySpan<byte> key, int low, int high, out int separator)
        {
            separator = 0;
            var prefixLength = node.PrefixLength;
            var word = Window(key, prefixLength);
            if (node.IsLeaf)
            {
                var leaf = Unsafe.As<Leaf>(node);
                var words = leaf.Words;
                var keys = leaf.Keys;
                var count = leaf.Count;
                var slot = Bound(words, keys, count, key, prefixLength, false);
                words[slot..count].CopyTo(words[(slot + 1)..]);
                keys[slot..count].CopyTo(keys[(slot + 1)..]);
                words[slot] = word;
                keys[slot] = record;
                if (++leaf.Count <= LeafCapacity) return null;
                _table._faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeOrderedIndexSplit);

                var middle = leaf.Count / 2;
                var right = new Leaf { Count = leaf.Count - middle, PrefixLength = prefixLength };
                words[middle..leaf.Count].CopyTo(right.Words);
                keys[middle..leaf.Count].CopyTo(right.Keys);
                separator = keys[middle];
                right.Next = leaf.Next;
                right.Previous = leaf;
                if (leaf.Next is not null) leaf.Next.Previous = right;
                leaf.Next = right;
                leaf.Count = middle;
                GrowPrefix(leaf, low, separator);
                GrowPrefix(right, separator, high);
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
                var child = Insert(children[slot]!, record, key, childLow, childHigh, out var childSeparator);
                if (child is null) return null;
                words[slot..count].CopyTo(words[(slot + 1)..]);
                keys[slot..count].CopyTo(keys[(slot + 1)..]);
                children[(slot + 1)..(count + 1)].CopyTo(children[(slot + 2)..]);
                words[slot] = Window(childSeparator, prefixLength);
                keys[slot] = childSeparator;
                children[slot + 1] = child;
                if (++branch.Count <= BranchCapacity) return null;
                _table._faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeOrderedIndexSplit);

                var middle = branch.Count / 2;
                var right = new Branch { Count = branch.Count - middle - 1, PrefixLength = prefixLength };
                separator = keys[middle];
                words[(middle + 1)..branch.Count].CopyTo(right.Words);
                keys[(middle + 1)..branch.Count].CopyTo(right.Keys);
                children[(middle + 1)..(branch.Count + 1)].CopyTo(right.Children);
                children[(middle + 1)..(branch.Count + 1)].Clear();
                branch.Count = middle;
                GrowPrefix(branch, low, separator);
                GrowPrefix(right, separator, high);
                return right;
            }
        }

        /// <summary>Gets the prefix length of a node between fences <paramref name="low"/> and <paramref name="high"/>.</summary>
        private int PrefixLength(int low, int high) =>
            low == NoFence || high == NoFence ? _prefixLength : Key(low).CommonPrefixLength(Key(high));

        /// <summary>Lengthens the prefix of a node that a split narrowed to the fences <paramref name="low"/> and <paramref name="high"/>.</summary>
        private void GrowPrefix(Node node, int low, int high)
        {
            var prefixLength = PrefixLength(low, high);
            if (prefixLength > node.PrefixLength) SetPrefix(node, prefixLength);
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

        private int Bound(Span<ulong> words, Span<int> keys, int count, ReadOnlySpan<byte> key, int prefixLength, bool upper)
        {
            var target = Window(key, prefixLength);
            var low = 0;
            var high = count;
            while (low < high)
            {
                var middle = low + ((high - low) >> 1);
                var word = words[middle];
                int comparison;
                if (word != target)
                {
                    comparison = word < target ? -1 : 1;
                }
                else
                {
                    comparison = CompareTied(keys[middle], key, prefixLength);
                }
                if (comparison < 0 || upper && comparison == 0) low = middle + 1;
                else high = middle;
            }
            return low;
        }

        public void Dispose()
        {
        }

        public abstract class Node(bool isLeaf)
        {
            public readonly bool IsLeaf = isLeaf;
            public int Count;

            /// <summary>The leading bytes that every key a search routes to the node shares, which its windows skip.</summary>
            public int PrefixLength;
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
