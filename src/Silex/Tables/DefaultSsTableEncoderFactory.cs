namespace Silex.Tables;

public sealed class DefaultSsTableEncoderFactory : ISsTableEncoderFactory
{
    public ISsTableEncoder<TKey, TValue> Create<TKey, TValue>() => new DefaultSsTableEncoder<TKey, TValue>();
}
