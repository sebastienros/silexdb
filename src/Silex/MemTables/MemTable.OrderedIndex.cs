using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Silex.MemTables;

internal sealed partial class MemTable
{
    /// <summary>
    /// A B+-tree of record positions in key order. Each node is one object that holds, next to its record
    /// positions, the prefix word of each key, so a search reads the node's own lines and touches a record only
    /// where two prefix words tie.
    /// </summary>
    internal sealed class OrderedIndex : IDisposable
    {
        internal const int LeafCapacity = 256;
        internal const int BranchCapacity = 32;
        private readonly MemTable _table;
        private Node _root;
        private long _version;
        public long Version => _version;
        internal Node Root => _root;

        public OrderedIndex(MemTable table, ReadOnlySpan<int> records)
        {
            _table = table;
            var height = 0;
            long capacity = LeafCapacity;
            while (capacity < records.Length)
            {
                capacity *= BranchCapacity + 1;
                height++;
            }
            Leaf? previous = null;
            _root = Build(records, height, capacity, ref previous);
        }

        private Node Build(ReadOnlySpan<int> records, int height, long capacity, ref Leaf? previous)
        {
            var tableRecords = _table._records;
            if (height == 0)
            {
                var leaf = new Leaf { Count = records.Length };
                records.CopyTo(leaf.Keys);
                var words = leaf.Words;
                for (var i = 0; i < records.Length; i++) words[i] = tableRecords[records[i]].KeyPrefixWord;
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
                node.Children[i] = Build(records[..count], height - 1, childCapacity, ref previous);
                if (i != 0)
                {
                    node.Keys[i - 1] = records[0];
                    node.Words[i - 1] = tableRecords[records[0]].KeyPrefixWord;
                }
                records = records[count..];
            }
            return node;
        }

        public Cursor Begin(bool backwards)
        {
            var node = _root;
            while (!node.IsLeaf)
            {
                var branch = Unsafe.As<Branch>(node);
                node = branch.Children[backwards ? branch.Count : 0]!;
            }
            var leaf = Unsafe.As<Leaf>(node);
            return new Cursor(this, leaf, backwards ? leaf.Count - 1 : 0);
        }

        public Cursor Seek(ReadOnlySpan<byte> key, bool inclusive, bool backwards)
        {
            var node = _root;
            var keyPrefix = ReadKeyPrefixWord(key);
            while (!node.IsLeaf)
            {
                var branch = Unsafe.As<Branch>(node);
                node = branch.Children[Bound(branch.Words, branch.Keys, branch.Count, key, keyPrefix, true)]!;
            }
            var leaf = Unsafe.As<Leaf>(node);
            var slot = Bound(leaf.Words, leaf.Keys, leaf.Count, key, keyPrefix, false);
            var equal = slot < leaf.Count && leaf.Words[slot] == keyPrefix && EqualsKey(leaf.Keys[slot], key);
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

        public void Insert(int record)
        {
            _version++;
            var key = Key(record);
            var keyPrefix = _table._records[record].KeyPrefixWord;
            var right = Insert(_root, record, key, keyPrefix, out var separator, out var separatorWord);
            if (right is null) return;
            var root = new Branch { Count = 1 };
            root.Keys[0] = separator;
            root.Words[0] = separatorWord;
            root.Children[0] = _root;
            root.Children[1] = right;
            _root = root;
        }

        private Node? Insert(Node node, int record, ReadOnlySpan<byte> key, ulong keyPrefix, out int separator, out ulong separatorWord)
        {
            separator = 0;
            separatorWord = 0;
            if (node.IsLeaf)
            {
                var leaf = Unsafe.As<Leaf>(node);
                var words = leaf.Words;
                var keys = leaf.Keys;
                var count = leaf.Count;
                var slot = Bound(words, keys, count, key, keyPrefix, false);
                words[slot..count].CopyTo(words[(slot + 1)..]);
                keys[slot..count].CopyTo(keys[(slot + 1)..]);
                words[slot] = keyPrefix;
                keys[slot] = record;
                if (++leaf.Count <= LeafCapacity) return null;
                _table._faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeOrderedIndexSplit);

                var middle = leaf.Count / 2;
                var right = new Leaf { Count = leaf.Count - middle };
                words[middle..leaf.Count].CopyTo(right.Words);
                keys[middle..leaf.Count].CopyTo(right.Keys);
                separator = keys[middle];
                separatorWord = words[middle];
                right.Next = leaf.Next;
                right.Previous = leaf;
                if (leaf.Next is not null) leaf.Next.Previous = right;
                leaf.Next = right;
                leaf.Count = middle;
                return right;
            }
            else
            {
                var branch = Unsafe.As<Branch>(node);
                var words = branch.Words;
                var keys = branch.Keys;
                var children = branch.Children;
                var count = branch.Count;
                var slot = Bound(words, keys, count, key, keyPrefix, true);
                var child = Insert(children[slot]!, record, key, keyPrefix, out var childSeparator, out var childSeparatorWord);
                if (child is null) return null;
                words[slot..count].CopyTo(words[(slot + 1)..]);
                keys[slot..count].CopyTo(keys[(slot + 1)..]);
                children[(slot + 1)..(count + 1)].CopyTo(children[(slot + 2)..]);
                words[slot] = childSeparatorWord;
                keys[slot] = childSeparator;
                children[slot + 1] = child;
                if (++branch.Count <= BranchCapacity) return null;
                _table._faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeOrderedIndexSplit);

                var middle = branch.Count / 2;
                var right = new Branch { Count = branch.Count - middle - 1 };
                separator = keys[middle];
                separatorWord = words[middle];
                words[(middle + 1)..branch.Count].CopyTo(right.Words);
                keys[(middle + 1)..branch.Count].CopyTo(right.Keys);
                children[(middle + 1)..(branch.Count + 1)].CopyTo(right.Children);
                children[(middle + 1)..(branch.Count + 1)].Clear();
                branch.Count = middle;
                return right;
            }
        }

        private ReadOnlySpan<byte> Key(int record) => _table._arena.GetSpan(_table._records[record].Key);

        private bool EqualsKey(int record, ReadOnlySpan<byte> key)
        {
            ref var rec = ref _table._records[record];
            return rec.Key.Length == key.Length && (key.Length <= 8 || _table._arena.GetSpan(rec.Key)[8..].SequenceEqual(key[8..]));
        }

        /// <summary>Compares the key of <paramref name="record"/> with <paramref name="key"/>, whose prefix words are equal.</summary>
        private int CompareTied(int record, ReadOnlySpan<byte> key)
        {
            ref var rec = ref _table._records[record];
            if (rec.Key.Length <= 8 && key.Length <= 8)
            {
                return rec.Key.Length.CompareTo(key.Length);
            }
            return _table._arena.GetSpan(rec.Key).SequenceCompareTo(key);
        }

        private int Bound(Span<ulong> words, Span<int> keys, int count, ReadOnlySpan<byte> key, ulong keyPrefix, bool upper)
        {
            var low = 0;
            var high = count;
            while (low < high)
            {
                var middle = low + ((high - low) >> 1);
                var word = words[middle];
                int comparison;
                if (word != keyPrefix)
                {
                    comparison = word < keyPrefix ? -1 : 1;
                }
                else
                {
                    comparison = CompareTied(keys[middle], key);
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
        }

        public sealed class Leaf() : Node(true)
        {
            public Leaf? Previous;
            public Leaf? Next;
            private LeafWords _words;
            private LeafKeys _keys;

            /// <summary>The prefix word of each key, in key order.</summary>
            public Span<ulong> Words => MemoryMarshal.CreateSpan(ref Unsafe.As<LeafWords, ulong>(ref _words), LeafCapacity + 1);

            /// <summary>The record position of each key, in key order.</summary>
            public Span<int> Keys => MemoryMarshal.CreateSpan(ref Unsafe.As<LeafKeys, int>(ref _keys), LeafCapacity + 1);
        }

        public sealed class Branch() : Node(false)
        {
            private BranchWords _words;
            private BranchKeys _keys;
            private BranchChildren _children;

            /// <summary>The prefix word of each separator.</summary>
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
