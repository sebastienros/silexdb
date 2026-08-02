namespace Silex.MemTables;

internal interface IRawBytesMemTable
{
    void PutRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
    void DeleteRaw(ReadOnlySpan<byte> key);
}
