namespace Silex.Tables;

/// <summary>
/// Creates an <see cref="ISsTableEncoder{T, U}"/> instance.
/// </summary>
public interface ISsTableEncoderFactory
{
    public ISsTableEncoder<TKey, TValue> Create<TKey, TValue>();
}
