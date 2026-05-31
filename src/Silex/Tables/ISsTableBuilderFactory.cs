namespace Silex.Tables;

using Silex.Blocks;
using Silex.BloomFilters;

public interface ISsTableBuilderFactory
{
    ISsTableBuilder<TKey> CreateSsTableBuilder<TKey>(string path, ISsTableEncoder<TKey> tableEncoder, IBlockEncoder<TKey> blockEncoder, IBloomFilterFactory bloomFilterFactory, int count);
}
