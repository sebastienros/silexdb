using System.Buffers;

namespace Silex;

internal struct MemTableEntry
{
    public IMemoryOwner<byte> MemoryOwner { get; set; }
    public int Size { get; set; }
    public readonly Memory<byte> Memory => MemoryOwner.Memory[..Size];
}
