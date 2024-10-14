namespace Silex.Test;

using Silex.Collections;

public class LockFreeQueueTests
{
    [Fact]
    public void ShouldQueueAndDequeueItems()
    {
        var queue = new LockFreeQueue<int>();

        Assert.True(queue.IsEmpty);

        queue.Enqueue(0);

        Assert.False(queue.IsEmpty);

        queue.Enqueue(1);

        Assert.False(queue.IsEmpty);

        Assert.Equal(0, queue.Dequeue());

        Assert.False(queue.IsEmpty);

        Assert.Equal(1, queue.Dequeue());

        Assert.True(queue.IsEmpty);
    }

    [Fact]
    public void DequeueEmptyShouldReturnDefaultTicket()
    {
        var queue = new LockFreeQueue<int>();

        Assert.Equal(0, queue.Dequeue());
        Assert.Equal(0, queue.Dequeue());

        queue.Enqueue(1);
        Assert.Equal(1, queue.Dequeue());
        Assert.Equal(0, queue.Dequeue());
    }
}
