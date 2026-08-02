namespace Silex;

/// <summary>
/// Describes one put or delete in a <see cref="LsmStorage.WriteBatch"/> call.
/// </summary>
public readonly struct WriteBatchEntry
{
    private WriteBatchEntry(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, bool isDelete)
    {
        Key = key;
        Value = value;
        IsDelete = isDelete;
    }

    public ReadOnlyMemory<byte> Key { get; }

    public ReadOnlyMemory<byte> Value { get; }

    public bool IsDelete { get; }

    public static WriteBatchEntry Put(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value) =>
        new(key, value, isDelete: false);

    public static WriteBatchEntry Delete(ReadOnlyMemory<byte> key) =>
        new(key, ReadOnlyMemory<byte>.Empty, isDelete: true);
}
