using System.Buffers;

namespace Silex.MemTables;

internal sealed partial class MemTable
{
    private sealed class OrderedIndex : IDisposable
    {
        private const int LeafCapacity = 256;
        private const int BranchCapacity = 32;
        private readonly MemTable _table;
        private Node _root;

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
            Node? previous = null;
            _root = Build(records, height, capacity, ref previous);
        }

        private static Node Build(ReadOnlySpan<int> records, int height, long capacity, ref Node? previous)
        {
            var node = new Node(height == 0);
            try
            {
                if (height == 0)
                {
                    node.Count = records.Length;
                    records.CopyTo(node.Keys);
                    node.Previous = previous;
                    if (previous is not null) previous.Next = node;
                    previous = node;
                    return node;
                }
                var childCapacity = capacity / (BranchCapacity + 1);
                var children = (int)((records.Length + childCapacity - 1) / childCapacity);
                node.Count = children - 1;
                for (var i = 0; i < children; i++)
                {
                    var count = (int)Math.Min(childCapacity, records.Length);
                    var child = Build(records[..count], height - 1, childCapacity, ref previous);
                    node.Children![i] = child;
                    if (i != 0) node.Keys[i - 1] = Minimum(child);
                    records = records[count..];
                }
                return node;
            }
            catch
            {
                Dispose(node);
                throw;
            }
        }

        private static int Minimum(Node node)
        {
            while (node.Children is not null) node = node.Children[0]!;
            return node.Keys[0];
        }

        public Cursor Begin(bool backwards)
        {
            var node = _root;
            while (node.Children is not null) node = node.Children[backwards ? node.Count : 0]!;
            return new Cursor(node, backwards ? node.Count - 1 : 0);
        }

        public Cursor Seek(ReadOnlySpan<byte> key, bool inclusive, bool backwards)
        {
            var node = _root;
            while (node.Children is not null) node = node.Children[Bound(node, key, true)]!;
            var slot = Bound(node, key, false);
            var equal = slot < node.Count && Key(node.Keys[slot]).SequenceEqual(key);
            if (backwards)
            {
                if (!equal || !inclusive) slot--;
                if (slot < 0 && node.Previous is { } previous) return new Cursor(previous, previous.Count - 1);
            }
            else
            {
                if (equal && !inclusive) slot++;
                if (slot == node.Count && node.Next is { } next) return new Cursor(next, 0);
            }
            return new Cursor(node, slot);
        }

        public void Insert(int record)
        {
            var right = Insert(_root, record, out var separator);
            if (right is null) return;
            var root = new Node(false) { Count = 1 };
            root.Keys[0] = separator;
            root.Children![0] = _root;
            root.Children[1] = right;
            _root = root;
        }

        private Node? Insert(Node node, int record, out int separator)
        {
            separator = 0;
            var slot = Bound(node, Key(record), node.Children is not null);
            if (node.Children is null)
            {
                node.Keys.AsSpan(slot, node.Count - slot).CopyTo(node.Keys.AsSpan(slot + 1));
                node.Keys[slot] = record;
            }
            else
            {
                var child = Insert(node.Children[slot]!, record, out var childSeparator);
                if (child is null) return null;
                node.Keys.AsSpan(slot, node.Count - slot).CopyTo(node.Keys.AsSpan(slot + 1));
                node.Children.AsSpan(slot + 1, node.Count - slot).CopyTo(node.Children.AsSpan(slot + 2));
                node.Keys[slot] = childSeparator;
                node.Children[slot + 1] = child;
            }
            node.Count++;
            var leaf = node.Children is null;
            if (node.Count <= (leaf ? LeafCapacity : BranchCapacity)) return null;
            var middle = node.Count / 2;
            var right = new Node(leaf);
            if (leaf)
            {
                right.Count = node.Count - middle;
                node.Keys.AsSpan(middle, right.Count).CopyTo(right.Keys);
                separator = right.Keys[0];
                right.Next = node.Next;
                right.Previous = node;
                if (node.Next is not null) node.Next.Previous = right;
                node.Next = right;
            }
            else
            {
                separator = node.Keys[middle];
                right.Count = node.Count - middle - 1;
                node.Keys.AsSpan(middle + 1, right.Count).CopyTo(right.Keys);
                node.Children!.AsSpan(middle + 1, right.Count + 1).CopyTo(right.Children);
                node.Children.AsSpan(middle + 1, right.Count + 1).Clear();
            }
            node.Count = middle;
            return right;
        }

        private ReadOnlySpan<byte> Key(int record) => _table._arena.GetSpan(_table._records[record].Key);

        private int Bound(Node node, ReadOnlySpan<byte> key, bool upper)
        {
            var low = 0;
            var high = node.Count;
            while (low < high)
            {
                var middle = low + ((high - low) >> 1);
                var comparison = Key(node.Keys[middle]).SequenceCompareTo(key);
                if (comparison < 0 || upper && comparison == 0) low = middle + 1;
                else high = middle;
            }
            return low;
        }

        public void Dispose() => Dispose(_root);

        private static void Dispose(Node node)
        {
            if (node.Children is not null)
            {
                for (var i = 0; i <= node.Count; i++)
                    if (node.Children[i] is { } child) Dispose(child);
                ArrayPool<Node?>.Shared.Return(node.Children, clearArray: true);
            }
            ArrayPool<int>.Shared.Return(node.Keys);
        }

        public sealed class Node
        {
            public readonly int[] Keys;
            public readonly Node?[]? Children;
            public int Count;
            public Node? Previous;
            public Node? Next;

            public Node(bool leaf)
            {
                Keys = ArrayPool<int>.Shared.Rent((leaf ? LeafCapacity : BranchCapacity) + 1);
                if (!leaf)
                {
                    try
                    {
                        Children = ArrayPool<Node?>.Shared.Rent(BranchCapacity + 2);
                        Children.AsSpan().Clear();
                    }
                    catch
                    {
                        ArrayPool<int>.Shared.Return(Keys);
                        throw;
                    }
                }
            }
        }

        public struct Cursor
        {
            private Node _node;
            private int _slot;
            public Cursor(Node node, int slot) { _node = node; _slot = slot; }
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
