namespace Silex.Tables;

using Silex.Blocks;
using Silex.BloomFilters;

internal interface ISsTableBuilderFactory
{
    ISsTableBuilder CreateSsTableBuilder(
        string path,
        ISsTableEncoder tableEncoder,
        IBlockEncoder blockEncoder,
        IBloomFilterFactory bloomFilterFactory,
        int count,
        SstCompression compression,
        int compressionLevel,
        double minimumCompressionSavingsPercent);
}
