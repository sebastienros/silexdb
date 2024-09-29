using System;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Xml.Linq;

namespace Silex.Collections;

internal sealed class SkipList<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
{
    private static readonly Node NullNode = new(0, default!, default!);
    private const double _probability = 0.5;

    private readonly Node _head;
    private int _count;
    private readonly Random _random;
    private readonly IComparer<TKey> _comparer;

    // Only to detect concurrent write accesses in DEBUG
    private volatile bool _writing = false;

    public SkipList() : this(Comparer<TKey>.Default)
    { 
    }

    public SkipList(IComparer<TKey> comparer) : this(comparer, Random.Shared)
    {
    }

    public SkipList(IComparer<TKey> comparer, Random random)
    {
        ArgumentNullException.ThrowIfNull(comparer);
        ArgumentNullException.ThrowIfNull(random);

        _head = new Node(1, default!, default!);
        _count = 0;
        _random = random;
        _comparer = comparer;
    }

    public int Height => _head.Height;

    public int Count => _count;

    public bool ContainsKey(TKey key)
    {
        EnsureNotWriting();

        return FindNode(key) != NullNode;
    }

    public bool TryGetValue(TKey key, out TValue value)
    {
        EnsureNotWriting();

        var node = FindNode(key);

        value = node.Value;

        if (node == NullNode)
        {
            return false;
        }

        return true;
    }

    public TValue this[TKey key]
    {
        get
        {
            EnsureNotWriting();

            // If the key is not found this will be NullNode.Value which is default(TValue)!
            return FindNode(key).Value;
        }

        set
        {
            EnterWriteSection();

            var current = FindNode(key);

            if (current != NullNode)
            {
                current.Value = value;
            }
            else
            {
                AddInternal(key, value);
            }

            ExitWriteSection();
        }
    }

    public void Add(TKey key, TValue value)
    {
        EnterWriteSection();

        AddInternal(key, value);

        ExitWriteSection();
    }

    public bool TryRemove(TKey key, out TValue value)
    {
        EnterWriteSection();

        var updates = BuildUpdateTable(key);
        var current = updates[0][0];

        if (current != NullNode && _comparer.Compare(current.Key, key) == 0)
        {
            _count--;

            // We found the data to delete
            for (var i = 0; i < _head.Height; i++)
            {
                if (updates[i][i] != current)
                {
                    break;
                }
                else
                {
                    updates[i][i] = current[i];
                }
            }

            // Finally, see if we need to trim the height of the list
            if (_head[_head.Height - 1] == NullNode)
            {
                // We removed the single, tallest item... reduce the list height
                _head.DecrementHeight();
            }

            // Item removed
            value = current.Value;

            ExitWriteSection();
            return true;
        }
        else
        {
            // The data to delete wasn't found
            value = NullNode.Value;

            ExitWriteSection();
            return false;
        }
    }

    public void CopyTo(Array array, int index)
    {
        EnsureNotWriting();

        ArgumentNullException.ThrowIfNull(array);
        ArgumentOutOfRangeException.ThrowIfLessThan(0, index);

        var i = index;

        var node = _head[0];
        while (node != NullNode)
        {
            EnsureNotWriting();

            array.SetValue(node.Value, i);
            node = node[0];
            i++;
        }
    }

    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
    {
        EnsureNotWriting();

        var node = _head[0];
        while (node != NullNode)
        {
            EnsureNotWriting();

            yield return new KeyValuePair<TKey, TValue>(node.Key, node.Value);
            node = node[0];
        }
    }

    /// <inheritdoc/>
    public void Clear()
    {
        EnterWriteSection();

        _count = 0;
        for (var c = 0; c < _head.Height; c++)
        {
            _head[c] = NullNode;
        }

        ExitWriteSection();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        EnsureNotWriting();

        return GetEnumerator();
    }

    /// <summary>
    /// Gets an iterator for all the entries with a key greater or equal to <paramref name="start"/>.
    /// </summary>
    /// <param name="start"></param>
    /// <returns></returns>
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator(TKey start)
    {
        EnsureNotWriting();
        
        var node = FindNode(start, includeClosest: true);

        while (node != NullNode)
        {
            yield return new KeyValuePair<TKey, TValue>(node.Key, node.Value);
            node = node[0];
        }
    }

    /// <summary>
    /// Gets an iterator for all the entries with a key greater or equal to <paramref name="start"/> and lower than <paramref name="end"/>.
    /// </summary>
    /// <param name="start"></param>
    /// <param name="end"></param>
    /// <returns></returns>
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator(TKey start, TKey end)
    {
        EnsureNotWriting();

        var node = FindNode(start, includeClosest: true);
        var endNode = FindNode(end, includeClosest: true);

        while (node != NullNode && node != endNode)
        {
            yield return new KeyValuePair<TKey, TValue>(node.Key, node.Value);
            node = node[0];
        }

        // Return the last node if the key matches exactly
        if (node == endNode && _comparer.Compare(node.Key, end) == 0)
        {
            yield return new KeyValuePair<TKey, TValue>(node.Key, node.Value);
        }
    }

    public bool TryGetNext(TKey key, out TKey result)
    {
        EnsureNotWriting();

        // Seek first element, either exact match or previous one

        var currentStart = _head;
        var node = NullNode;
        
        for (var i = _head.Height - 1; i >= 0; i--)
        {
            while (currentStart[i] != NullNode)
            {
                var results = _comparer.Compare(currentStart[i].Key, key);
                if (results == 0)
                {
                    // Exact, get next item
                    node = currentStart[i][0];

                    // Exit the outer for loop
                    i = -1;

                    // Exit the while loop
                    break;
                }
                else if (results < 0)
                {
                    // The element is to the left, so move down a level
                    currentStart = currentStart[i];
                    node = currentStart[0];
                }
                else
                {
                    if (i == 0 && node == NullNode)
                    {
                        node = _head[0];
                    }

                    // Exit while loop, because the element is to the right of this node, at (or lower than) the current level
                    break;
                }
            }
        }

        result = node.Key;

        // If the element was not found, exit
        if (node == NullNode)
        {
            return false;
        }

        return true;
    }

    [Conditional("DEBUG")]
    private void EnterWriteSection()
    {
        Debug.Assert(!_writing);
        _writing = true;
    }

    [Conditional("DEBUG")]
    private void ExitWriteSection()
    {
        Debug.Assert(_writing);
        _writing = false;
    }

    [Conditional("DEBUG")]
    private void EnsureNotWriting()
    {
        Debug.Assert(!_writing);
    }

    private void AddInternal(TKey key, TValue value)
    {
        var updates = BuildUpdateTable(key);

        var current = updates[0];

        // Is the key already present?
        if (current[0] != NullNode && _comparer.Compare(current[0].Key, key) == 0)
        {
            return;
        }

        // Create a new node
        var n = new Node(ChooseRandomHeight(_head.Height + 1), key, value);

        // Increment the count of elements in the skip list
        _count++;

        // if the node's level is greater than the head's level, increase the head's level
        if (n.Height > _head.Height)
        {
            _head.IncrementHeight();
            _head[_head.Height - 1] = n;
        }

        // Splice the new node into the list
        for (int i = 0; i < n.Height; i++)
        {
            if (i < updates.Length)
            {
                n[i] = updates[i][i];
                updates[i][i] = n;
            }
        }
    }

    private Node[] BuildUpdateTable(TKey key)
    {
        var updates = new Node[_head.Height];
        var current = _head;

        // Determine the nodes that need to be updated at each level
        for (var i = _head.Height - 1; i >= 0; i--)
        {
            while (current[i] != NullNode && _comparer.Compare(current[i].Key, key) < 0)
            {
                current = current[i];
            }

            updates[i] = current;
        }

        return updates;
    }

    private int ChooseRandomHeight(int maxLevel)
    {
        var level = 1;
        while (_random.NextDouble() < _probability && level < maxLevel)
        {
            level++;
        }

        return level;
    }

    private Node FindNode(TKey key, bool includeClosest = false)
    {
        var current = _head;
        var closest = NullNode;

        for (int i = _head.Height - 1; i >= 0; i--)
        {
            while (current[i] != NullNode)
            {
                int results = _comparer.Compare(current[i].Key, key);
                if (results == 0)
                {
                    // We found the element
                    return current[i];
                }
                else if (results < 0)
                {
                    // The element is to the left, so move down a level
                    current = current[i];
                    closest = current[0];
                }
                else
                {
                    if (i == 0 && closest == NullNode)
                    {
                        closest = _head[0];
                    }

                    // Exit while loop, because the element is to the right of this node, at (or lower than) the current level
                    break;
                }
            }
        }

        if (includeClosest)
        {
            return closest;
        }

        // Element not found
        return NullNode;
    }

    private class Node
    {
        private readonly List<Node> _neighbors;

        public Node(int height, TKey key, TValue value)
        {
            Key = key;
            Value = value;

            _neighbors = new(height);

            // Add the specified number of items
            for (int i = 0; i < height; i++)
            {
                _neighbors.Add(NullNode);
            }
        }

        public TKey Key { get; set; }

        public TValue Value { get; set; }

        public int Height => _neighbors.Count;

        public Node this[int index]
        {
            get { return _neighbors[index]; }
            set { _neighbors[index] = value; }
        }

        public void IncrementHeight()
        {
            _neighbors.Add(NullNode);
        }

        public void DecrementHeight()
        {
            if (Height == 1)
            {
                return;
            }

            _neighbors.RemoveAt(_neighbors.Count - 1);
        }
    }
}
