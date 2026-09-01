using BenchmarkDotNet.Attributes;
using Silex.Blocks;
using Silex.Tables;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class BlockCacheBenchmarks
{
    private const int OperationsPerInvoke = 1_000;
    private static readonly BlockCacheKey Key = new(1, 0);

    private BlockCache _cache = null!;

    [GlobalSetup]
    public void Setup()
    {
        _cache = new BlockCache(1.MiB());
        using var lease = _cache.GetOrLoadAsync(Key, default(BlockLoader)).Result;
    }

    [GlobalCleanup]
    public void Cleanup() => _cache.Dispose();

    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public int RepeatedHit()
    {
        var found = 0;

        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            using var lease = _cache.GetOrLoadAsync(Key, default(BlockLoader)).Result;
            found += lease.Block is null ? 0 : 1;
        }

        return found;
    }

    private readonly struct BlockLoader : IBlockLoader
    {
        public Block Load(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var builder = new BlockBuilder(new DefaultBlockEncoder());
            builder.Add(ByteSlice.FromMemory(new byte[] { 1 }), ByteSlice.FromMemory(new byte[] { 1 }));
            return builder.BuildBlock();
        }
    }
}
