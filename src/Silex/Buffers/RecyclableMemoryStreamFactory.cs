using Microsoft.IO;

namespace Silex.Buffers;

internal class RecyclableMemoryStreamFactory
{
    public static readonly RecyclableMemoryStreamManager Shared = new();

}
