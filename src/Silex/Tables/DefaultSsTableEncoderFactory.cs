namespace Silex.Tables;

internal sealed class DefaultSsTableEncoderFactory : ISsTableEncoderFactory
{
    public ISsTableEncoder Create() => new DefaultSsTableEncoder();
}
