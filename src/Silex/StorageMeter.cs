using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Silex;

/// <summary>
/// Exposes the cheap, allocation-free engine counters through <see cref="System.Diagnostics.Metrics"/> under the
/// meter name <see cref="MeterName"/>. Instruments are observable, so nothing is recorded on the hot path; values
/// are read from the same counters as <c>GetStatisticsAsync</c> when a listener polls. Each open store contributes
/// measurements tagged with <c>silex.storage.path</c>. Asynchronous or derived signals (pressure flags,
/// range estimates, snapshot ages) are available only through the statistics API.
/// </summary>
public static class StorageMeter
{
    public const string MeterName = "Silex.Storage";

    private static readonly ConcurrentDictionary<StorageMetrics, string> _stores = new();
    private static readonly Meter _meter = new(MeterName);

    static StorageMeter()
    {
        _meter.CreateObservableCounter("silex.storage.write.operations", () => Observe(m => m.WriteOperations), "{operation}", "Committed write operations.");
        _meter.CreateObservableCounter("silex.storage.write.user_bytes", () => Observe(m => m.UserBytesWritten), "By", "Logical key and value bytes written by callers.");
        _meter.CreateObservableCounter("silex.storage.wal.bytes", () => Observe(m => m.WalBytesWritten), "By", "Bytes appended to write-ahead logs.");
        _meter.CreateObservableCounter("silex.storage.wal.syncs", () => Observe(m => m.WalSync.Count), "{sync}", "Write-ahead log synchronizations.");
        _meter.CreateObservableCounter("silex.storage.flush.completed", () => Observe(m => m.Flush.ToStatistics().Duration.Count), "{flush}", "Completed memtable flushes.");
        _meter.CreateObservableCounter("silex.storage.compaction.completed", () => Observe(m => m.Compaction.ToStatistics().Duration.Count), "{compaction}", "Completed compactions.");
        _meter.CreateObservableCounter("silex.storage.background.failures", () => Observe(m => m.BackgroundFailures), "{failure}", "Background maintenance failures.");
        _meter.CreateObservableUpDownCounter("silex.storage.flush.running", () => Observe(m => m.Flush.IsRunning ? 1 : 0), "{flush}", "Flushes currently running.");
        _meter.CreateObservableUpDownCounter("silex.storage.compaction.running", () => Observe(m => m.Compaction.IsRunning ? 1 : 0), "{compaction}", "Compactions currently running.");
    }

    internal static void Register(StorageMetrics metrics, string path) => _stores[metrics] = path;

    internal static void Unregister(StorageMetrics metrics) => _stores.TryRemove(metrics, out _);

    private static IEnumerable<Measurement<long>> Observe(Func<StorageMetrics, long> read)
    {
        foreach (var (metrics, path) in _stores)
        {
            yield return new Measurement<long>(read(metrics), new KeyValuePair<string, object?>("silex.storage.path", path));
        }
    }
}
