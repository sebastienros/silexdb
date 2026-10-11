using System.Diagnostics;

namespace Silex;

/// <summary>
/// Allocation-free cumulative counters for one storage engine instance. Every update is a bounded number of
/// interlocked operations on preallocated fields; nothing here allocates on a hot path.
/// </summary>
internal sealed class StorageMetrics
{
    private long _writeOperations;
    private long _userBytesWritten;
    private long _walBytesWritten;
    private long _walSyncCount;
    private long _walSyncTicks;
    private long _walSyncMaxTicks;
    private long _walSyncLastTicks;
    private long _writerWaitCount;
    private long _writerWaitTicks;
    private long _writerWaitMaxTicks;
    private long _writerWaitLastTicks;
    private long _backgroundFailures;
    private Exception? _lastBackgroundFailure;
    private long _lastBackgroundFailureUtcTicks;

    public MaintenanceMetrics Flush { get; } = new();
    public MaintenanceMetrics Compaction { get; } = new();

    public long WriteOperations => Interlocked.Read(ref _writeOperations);
    public long UserBytesWritten => Interlocked.Read(ref _userBytesWritten);
    public long WalBytesWritten => Interlocked.Read(ref _walBytesWritten);
    public long BackgroundFailures => Interlocked.Read(ref _backgroundFailures);
    public Exception? LastBackgroundFailure => Volatile.Read(ref _lastBackgroundFailure);
    public long LastBackgroundFailureUtcTicks => Interlocked.Read(ref _lastBackgroundFailureUtcTicks);

    public void RecordWrite(long userBytes)
    {
        Interlocked.Increment(ref _writeOperations);
        Interlocked.Add(ref _userBytesWritten, userBytes);
    }

    public void RecordWalBytes(long bytes) => Interlocked.Add(ref _walBytesWritten, bytes);

    public void RecordWalSync(long startTimestamp)
    {
        var ticks = Stopwatch.GetTimestamp() - startTimestamp;
        Interlocked.Increment(ref _walSyncCount);
        Interlocked.Add(ref _walSyncTicks, ticks);
        AtomicMax(ref _walSyncMaxTicks, ticks);
        Interlocked.Exchange(ref _walSyncLastTicks, ticks);
    }

    public void RecordWriterWait(long startTimestamp)
    {
        var ticks = Stopwatch.GetTimestamp() - startTimestamp;
        Interlocked.Increment(ref _writerWaitCount);
        Interlocked.Add(ref _writerWaitTicks, ticks);
        AtomicMax(ref _writerWaitMaxTicks, ticks);
        Interlocked.Exchange(ref _writerWaitLastTicks, ticks);
    }

    public void RecordBackgroundFailure(Exception exception)
    {
        Volatile.Write(ref _lastBackgroundFailure, exception);
        Interlocked.Exchange(ref _lastBackgroundFailureUtcTicks, DateTime.UtcNow.Ticks);
        Interlocked.Increment(ref _backgroundFailures);
    }

    public DurationStatistics WalSync => DurationStatistics.FromTicks(
        Interlocked.Read(ref _walSyncCount), Interlocked.Read(ref _walSyncTicks),
        Interlocked.Read(ref _walSyncMaxTicks), Interlocked.Read(ref _walSyncLastTicks));

    public DurationStatistics WriterWait => DurationStatistics.FromTicks(
        Interlocked.Read(ref _writerWaitCount), Interlocked.Read(ref _writerWaitTicks),
        Interlocked.Read(ref _writerWaitMaxTicks), Interlocked.Read(ref _writerWaitLastTicks));

    internal static void AtomicMax(ref long location, long value)
    {
        var current = Volatile.Read(ref location);
        while (value > current)
        {
            var previous = Interlocked.CompareExchange(ref location, value, current);
            if (previous == current)
            {
                return;
            }
            current = previous;
        }
    }

    internal sealed class MaintenanceMetrics
    {
        private long _running;
        private long _completed;
        private long _ticks;
        private long _maxTicks;
        private long _lastTicks;
        private long _inputBytes;
        private long _outputBytes;

        public bool IsRunning => Interlocked.Read(ref _running) != 0;

        public void Begin() => Interlocked.Increment(ref _running);

        public void End() => Interlocked.Decrement(ref _running);

        public void RecordCompleted(long startTimestamp, long inputBytes, long outputBytes)
        {
            var ticks = Stopwatch.GetTimestamp() - startTimestamp;
            Interlocked.Add(ref _inputBytes, inputBytes);
            Interlocked.Add(ref _outputBytes, outputBytes);
            Interlocked.Add(ref _ticks, ticks);
            AtomicMax(ref _maxTicks, ticks);
            Interlocked.Exchange(ref _lastTicks, ticks);
            Interlocked.Increment(ref _completed);
        }

        public StorageMaintenanceStatistics ToStatistics() => new(
            DurationStatistics.FromTicks(Interlocked.Read(ref _completed), Interlocked.Read(ref _ticks),
                Interlocked.Read(ref _maxTicks), Interlocked.Read(ref _lastTicks)),
            IsRunning,
            Interlocked.Read(ref _inputBytes),
            Interlocked.Read(ref _outputBytes));
    }
}

/// <summary>
/// Lossy-contention-free event counter for hot read paths: increments are spread over cache-line separated
/// stripes chosen by the calling thread, and reading sums the stripes.
/// </summary>
internal sealed class StripedCounter
{
    private const int StripeCount = 16;
    private const int StrideLongs = 8;
    private readonly long[] _stripes = new long[StripeCount * StrideLongs];

    public void Increment() =>
        Interlocked.Increment(ref _stripes[(Environment.CurrentManagedThreadId & (StripeCount - 1)) * StrideLongs]);

    public long Value
    {
        get
        {
            long total = 0;
            for (var i = 0; i < StripeCount; i++)
            {
                total += Interlocked.Read(ref _stripes[i * StrideLongs]);
            }
            return total;
        }
    }
}
