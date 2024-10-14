namespace Silex.Collections;
internal sealed class LockFreeQueue<T>
{
    private Node _tail;
    internal Node _head;

    public LockFreeQueue()
    {
        _tail = new Node(default, null);
        _head = _tail;
    }

    public bool IsEmpty => Volatile.Read(ref _head.Next) == null;

    public void Enqueue(T item)
    {
        Node? oldTail = null;

        // Create the new node
        var node = new Node(item, null);

        // Loop until we have managed to update the tail's Next link 
        // to point to our new node
        var updatedNewLink = false;
        while (!updatedNewLink)
        {
            // Make local copies of the tail and its Next link, but in 
            // getting the latter use the local copy of the tail since
            // another thread may have changed the value of tail
            oldTail = Volatile.Read(ref _tail);
            Node? oldNext = oldTail.Next;

            // Providing that the tail field has not changed...
            if (_tail == oldTail)
            {
                // ...and its Next field is null
                if (oldNext == null)
                {
                    // ...try to update the tail's Next field
                    updatedNewLink = Interlocked.CompareExchange(ref _tail.Next, node, null) == null;
                }

                // If the tail's Next field was non-null, another thread
                // is in the middle of enqueuing a new node, so try and 
                // advance the tail to point to its Next node
                else
                {
                    Interlocked.CompareExchange(ref _tail, oldNext, oldTail);
                }
            }
        }

        // Try and update the tail field to point to our node; don't
        // worry if we can't, another thread will update it for us on
        // the next call to Enqueue()
        Interlocked.CompareExchange(ref _tail, node, oldTail);
    }

    public T? Dequeue()
    {
        var result = default(T);

        // Loop until we manage to advance the head, removing 
        // a node (if there are no nodes to dequeue, we'll exit
        // the method instead)
        var haveAdvancedHead = false;
        while (!haveAdvancedHead)
        {
            // Make local copies of the head, the tail, and the head's Next 
            // reference
            Node oldHead = Volatile.Read(ref _head);
            Node oldTail = Volatile.Read(ref _tail);
            Node? oldHeadNext = oldHead.Next;

            // Providing _that the head field has not changed...
            if (oldHead == _head)
            {
                // ...and it is equal to the tail field
                if (oldHead == oldTail)
                {
                    // ...and the head's Next field is null
                    if (oldHeadNext == null)
                    {
                        // ...then there is nothing to dequeue
                        return default;
                    }

                    // If the head's Next field is non-null and head was equal to the tail
                    // then we have a lagging tail: try and update it
                    Interlocked.CompareExchange(ref _tail, oldHeadNext, oldTail);
                }

                // Otherwise the head and tail fields are different
                else
                {
                    // Grab the item to dequeue, and then try to advance the head reference
                    result = oldHeadNext.Item;
                    haveAdvancedHead = Interlocked.CompareExchange(ref _head, oldHeadNext, oldHead) == oldHead;
                }
            }
        }

        return result;
    }

    internal sealed class Node
    {
        public Node(T? item, Node? next)
        {
            Item = item;
            Next = next;
        }

        public T? Item;
        public Node? Next;
    }
}
