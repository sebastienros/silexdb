using System.Buffers;

namespace Silex;

internal readonly struct MemoryOwner : IMemoryOwner<byte>
{
    private readonly Memory<byte> _memory;

    public MemoryOwner(Memory<byte> memory)
    {
        _memory = memory;
    }
    public readonly Memory<byte> Memory => _memory;

    public readonly void Dispose()
    {
    }
}
