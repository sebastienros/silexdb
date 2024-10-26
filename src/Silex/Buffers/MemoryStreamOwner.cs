using Microsoft.IO;
using System.Buffers;

namespace Silex.Buffers;

internal class MemoryStreamOwner : IMemoryOwner<byte>
{
    private readonly RecyclableMemoryStream _stream;
    private readonly Memory<byte> _memory;

    public MemoryStreamOwner(RecyclableMemoryStream stream)
    {
        _memory = stream.GetMemory();
        _stream = stream;
    }

    public Memory<byte> Memory => _memory;

    public void Dispose()
    {
        _stream.Dispose();
    }
}
