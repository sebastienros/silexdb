namespace Silex.Tables;

public class DefaultSsTableEncoderFactory : ISsTableEncoderFactory
{
    public ISsTableEncoder<TKey, TValue> Create<TKey, TValue>() => new DefaultSsTableEncoder<TKey, TValue>();
}
