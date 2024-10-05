using Silex.Blocks;
using System.Buffers;
using System.Diagnostics;

namespace Silex.Tables;

public class SsTableBuilder
{
    const string _tableNamePrefix = "sst";

    private readonly string _path;
    private readonly ISsTableEncoder _tableEncoder;
    private readonly IBlockEncoder _blockEncoder;
    private readonly long _tableSizeBytes;
    private readonly List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> _blockEntries = [];
    
    public SsTableBuilder(string path, ISsTableEncoder tableEncoder, IBlockEncoder blockEncoder, long tableSizeBytes)
    {
        _path = path;
        _tableEncoder = tableEncoder;
        _blockEncoder = blockEncoder;
        _tableSizeBytes = tableSizeBytes;
    }

    public void Clear()
    {
        _blockEntries.Clear();
    }

    public void AddEntry(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        _blockEntries.Add(new(key, value));
    }

    public async Task<IReadOnlyList<SsTable>> BuildTablesAsync(CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(_path))
        {
            Directory.CreateDirectory(_path);
        }

        var tables = new List<SsTable>();

        var index = 1;
        
        var blockBuilder = new BlockBuilder(_blockEncoder);

        List<BlockMetadata> blockMetadata = [];
        FileStream? stream = null;
        long offset = 0; // The current offset in the SST file
        var firstKey = ReadOnlyMemory<byte>.Empty;
        var lastKey = ReadOnlyMemory<byte>.Empty;
        string? filename = null;
        var isTableOpen = false;

        try
        {
            foreach (var entry in _blockEntries)
            {
                if (!isTableOpen)
                {
                    do
                    {
                        filename = Path.Combine(_path, $"{_tableNamePrefix}.{index++:000}");
                    } while (File.Exists(filename));

                    blockMetadata.Clear();
                    stream = File.Create(filename);
                    offset = 0;
                    blockBuilder.Clear();

                    isTableOpen = true;
                }

                Debug.Assert(stream != null);
                Debug.Assert(filename != null);

                blockBuilder.AddEntry(entry.Key, entry.Value);

                if (firstKey.IsEmpty)
                {
                    firstKey = entry.Key;
                }

                lastKey = entry.Key;

                if (blockBuilder.EstimatedSize >= _tableEncoder.BlockSize)
                {
                    var blockIndex = blockMetadata.Count;
                    using var block = await WriteBlockAsync(blockBuilder, blockMetadata, stream, blockIndex, offset, firstKey, lastKey, cancellationToken);
                    offset += block.Memory.Length;
                    blockBuilder.Clear();
                    firstKey = ReadOnlyMemory<byte>.Empty;
                }

                // Flush table on disk?
                if (offset + _tableEncoder.BlockSize >= _tableSizeBytes)
                {
                    await WriteTableMetadataAsync(blockMetadata, offset, stream, cancellationToken);
                    await stream.DisposeAsync();

                    // Create new table

                    var table = new SsTable(filename, blockMetadata, offset, _blockEncoder);
                    tables.Add(table);

                    isTableOpen = false;
                }
            }

            // Is there a table pending finalization?
            if (isTableOpen)
            {
                Debug.Assert(stream != null);

                // If there are entries to flush, create a final table
                if (blockBuilder.HasEntries)
                {
                    var blockIndex = blockMetadata.Count;
                    using var block = await WriteBlockAsync(blockBuilder, blockMetadata, stream, blockIndex++, offset, firstKey, lastKey, cancellationToken);
                    offset += block.Memory.Length;
                }

                await WriteTableMetadataAsync(blockMetadata, offset, stream, cancellationToken);
                await stream.DisposeAsync();

                Debug.Assert(filename != null);

                var table = new SsTable(filename, blockMetadata, offset, _blockEncoder);
                tables.Add(table);
            }
        }
        catch
        {
            if (stream != null)
            {
                await stream.DisposeAsync();
                stream = null;
            }
        }

        return tables;

        static async Task<Block> WriteBlockAsync(BlockBuilder blockBuilder, List<BlockMetadata> blockMetadata, FileStream stream, int index, long offset, ReadOnlyMemory<byte> firstKey, ReadOnlyMemory<byte> lastKey, CancellationToken cancellationToken)
        {
            var block = blockBuilder.BuildBlock();

            var m = new BlockMetadata()
            {
                Index = index,
                Offset = offset,
                FirstKey = firstKey,
                LastKey = lastKey
            };

            await stream.WriteAsync(block.Memory, cancellationToken);
            blockMetadata.Add(m);

            return block;
        }
    }

    private async Task WriteTableMetadataAsync(List<BlockMetadata> blockMetadata, long offset, FileStream stream, CancellationToken cancellationToken)
    {
        IMemoryOwner<byte>? memoryOwner = null;

        try
        {
            (memoryOwner, int length) = _tableEncoder.EncodeMetadata(blockMetadata, offset);
            await stream.WriteAsync(memoryOwner.Memory.Slice(0, length), cancellationToken);
        }
        finally
        {
            memoryOwner?.Dispose();
        }
    }
}
