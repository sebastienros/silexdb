namespace Silex;

/// <summary>
/// A reusable collection of byte-oriented mutations that can be written with one WAL write and flush
/// when the WAL is enabled.
/// </summary>
/// <remarks>
/// Keys and values are copied when added, so callers may reuse their input buffers immediately.
/// A committed batch is one WAL recovery unit: replay applies either every mutation or none of them.
/// </remarks>
public sealed class LsmWriteBatch
{
    internal readonly List<LsmWriteBatchEntry> Entries = [];

    /// <summary>
    /// Gets the number of mutations in this batch.
    /// </summary>
    public int Count => Entries.Count;

    /// <summary>
    /// Adds or replaces a value.
    /// </summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        Entries.Add(new LsmWriteBatchEntry(key.ToArray(), value.ToArray()));
    }

    /// <summary>
    /// Adds a deletion.
    /// </summary>
    public void Delete(ReadOnlySpan<byte> key)
    {
        Entries.Add(new LsmWriteBatchEntry(key.ToArray(), Value: null));
    }

    /// <summary>
    /// Removes every mutation so this instance can be reused.
    /// </summary>
    public void Clear()
    {
        Entries.Clear();
    }
}

internal readonly record struct LsmWriteBatchEntry(byte[] Key, byte[]? Value)
{
    public bool IsTombstone => Value is null;
}
