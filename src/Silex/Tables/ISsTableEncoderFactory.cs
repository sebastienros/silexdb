namespace Silex.Tables;

/// <summary>
/// Creates an <see cref="ISsTableEncoder{T}"/> instance.
/// </summary>
public interface ISsTableEncoderFactory
{
    public ISsTableEncoder<TKey> Create<TKey>();
}
