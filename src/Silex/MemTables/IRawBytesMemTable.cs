namespace Silex.MemTables;

internal interface IRawBytesMemTable
{
    void PutRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
}
