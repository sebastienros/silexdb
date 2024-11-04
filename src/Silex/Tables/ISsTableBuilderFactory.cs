namespace Silex.Tables;

using Silex.Blocks;
using Silex.BloomFilters;

public interface ISsTableBuilderFactory
{
    ISsTableBuilder<TKey, TValue> CreateSsTableBuilder<TKey, TValue>(string path, ISsTableEncoder<TKey, TValue> tableEncoder, IBlockEncoder<TKey, TValue> blockEncoder, IBloomFilterFactory bloomFilterFactory, int count);
}
