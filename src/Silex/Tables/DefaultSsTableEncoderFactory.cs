namespace Silex.Tables;

public sealed class DefaultSsTableEncoderFactory : ISsTableEncoderFactory
{
    public ISsTableEncoder<TKey> Create<TKey>() => new DefaultSsTableEncoder<TKey>();
}
