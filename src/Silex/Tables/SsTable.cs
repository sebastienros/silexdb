using Microsoft.Extensions.Caching.Memory;
using Silex.Blocks;
using System.Buffers;
using System.Buffers.Binary;

namespace Silex.Tables;

public class SsTable<TKey, TValue>
{
    private readonly long _id;
    private readonly string _filename;
    private readonly TKey? _firstKey;
    private readonly TKey? _lastKey;
    private readonly BlockBuilder<TKey, TValue> _blockBuilder;
    private static readonly WorkDispatcher<BlockCacheKey, Block<TKey, TValue>> _dispatcher = new();

    public SsTable(long id, string filename, IReadOnlyList<BlockMetadata<TKey>> blockMetadata, long metadataBlockOffset, BlockBuilder<TKey, TValue> blockBuilder)
    {
        _id = id;
        _filename = filename;
        BlockMetadata = blockMetadata;
        MetaBlockOffset = metadataBlockOffset;
        _blockBuilder = blockBuilder;

        if (blockMetadata.Count > 0)
        {
            _firstKey = BlockMetadata[0].FirstKey;
            _lastKey = BlockMetadata[BlockMetadata.Count - 1].LastKey;
        }
    }

    public IReadOnlyList<BlockMetadata<TKey>> BlockMetadata { get; } = [];

    public long MetaBlockOffset { get; }

    public string Filename => _filename;

    public TKey FirstKey => _firstKey!;

    public TKey LastKey => _lastKey!;

    public async Task<Block<TKey, TValue>?> ReadBlockAsync(int index, CancellationToken cancellationToken = default)
    {
        using var stream = File.OpenRead(Filename);
        var offset = BlockMetadata[index].Offset;
        
        // If there is a single block it ends at the metadata block
        var offsetEnd = BlockMetadata.Count > index + 1
            ? BlockMetadata[index + 1].Offset
            : MetaBlockOffset;

        var length = (int)(offsetEnd - offset);

        var buffer = ArrayPool<byte>.Shared.Rent(length)!;

        stream.Seek(offset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, length, cancellationToken);
        
        var block = _blockBuilder.Decode(new ReadOnlyMemory<byte>(buffer, 0, length));

        ArrayPool<byte>.Shared.Return(buffer);

        return block;
    }

    public Task<Block<TKey, TValue>?> ReadBlockCachedAsync(int index, IMemoryCache blockCache, MemoryCacheEntryOptions cacheEntryOptions, CancellationToken cancellationToken = default)
    {
        var key = new BlockCacheKey(_id, index);

        // Try without the dispatcher first since this is a cheap lookup
        if (blockCache.TryGetValue(key, out var block))
        {
            return Task.FromResult(block as Block<TKey, TValue>);
        }

        // Use a dispatcher to prevent cache stampede
        return _dispatcher.ScheduleAsync(key, (key) =>
        {
            return blockCache.GetOrCreateAsync(key, async entry =>
            {
                var block = await ReadBlockAsync(index, cancellationToken);

                if (block != null)
                {
                    entry.SetSize(block.Memory.Length);
                    entry.SetOptions(cacheEntryOptions);
                }

                return block;
            });
        });
    }

    public static async Task<SsTable<TKey, TValue>> LoadSsTableAsync(string filename, ISsTableEncoder<TKey, TValue> tableEncoder, BlockBuilder<TKey, TValue> blockBuilder, CancellationToken cancellationToken = default)
    {
        using var stream = File.OpenRead(filename);

        // Read the metadata block offset in the last four bytes
        stream.Seek(stream.Length - 4, SeekOrigin.Begin);
        var buffer = ArrayPool<byte>.Shared.Rent(4);
        Memory<byte> memory = buffer.AsMemory(0, 4);
        await stream.ReadExactlyAsync(memory, cancellationToken);
        var metaBlockOffset = BinaryPrimitives.ReadUInt32LittleEndian(buffer);

        // Read the metadata block content
        var metadataLength = stream.Length - 4 - metaBlockOffset;

        buffer = ArrayPool<byte>.Shared.Rent((int)metadataLength);
        stream.Seek(metaBlockOffset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, (int)metadataLength, cancellationToken);
        var blockMetadata = tableEncoder.DecodeMetadata(buffer, 0);
        ArrayPool<byte>.Shared.Return(buffer);

        var table = new SsTable<TKey, TValue>(IdGenerator.GetNextId(), filename, blockMetadata, metaBlockOffset, blockBuilder);
        
        return table;
    }

    private record struct BlockCacheKey(long Id, int Index);
}
