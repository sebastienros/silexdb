using System.Buffers.Binary;
using ArmCrc32 = System.Runtime.Intrinsics.Arm.Crc32;
using X86Crc32 = System.Runtime.Intrinsics.X86.Sse42;

namespace Silex.Wal;

internal static class Crc32C
{
    private const uint Polynomial = 0x82F63B78;

    public static uint Compute(ReadOnlySpan<byte> data)
    {
        if (X86Crc32.IsSupported)
        {
            return ComputeX86(data);
        }

        if (ArmCrc32.IsSupported)
        {
            return ComputeArm(data);
        }

        return ComputeSoftware(data);
    }

    internal static uint ComputeSoftware(ReadOnlySpan<byte> data)
    {
        var crc = uint.MaxValue;

        foreach (var value in data)
        {
            crc ^= value;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc >> 1) ^ (Polynomial & (uint)-(int)(crc & 1));
            }
        }

        return ~crc;
    }

    private static uint ComputeX86(ReadOnlySpan<byte> data)
    {
        var offset = 0;
        uint crc;

        if (X86Crc32.X64.IsSupported)
        {
            ulong crc64 = uint.MaxValue;
            while (data.Length - offset >= sizeof(ulong))
            {
                crc64 = X86Crc32.X64.Crc32(crc64, BinaryPrimitives.ReadUInt64LittleEndian(data[offset..]));
                offset += sizeof(ulong);
            }

            crc = (uint)crc64;
        }
        else
        {
            crc = uint.MaxValue;
        }

        while (data.Length - offset >= sizeof(uint))
        {
            crc = X86Crc32.Crc32(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[offset..]));
            offset += sizeof(uint);
        }

        if (data.Length - offset >= sizeof(ushort))
        {
            crc = X86Crc32.Crc32(crc, BinaryPrimitives.ReadUInt16LittleEndian(data[offset..]));
            offset += sizeof(ushort);
        }

        if (offset < data.Length)
        {
            crc = X86Crc32.Crc32(crc, data[offset]);
        }

        return ~crc;
    }

    private static uint ComputeArm(ReadOnlySpan<byte> data)
    {
        var offset = 0;
        var crc = uint.MaxValue;

        if (ArmCrc32.Arm64.IsSupported)
        {
            while (data.Length - offset >= sizeof(ulong))
            {
                crc = ArmCrc32.Arm64.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt64LittleEndian(data[offset..]));
                offset += sizeof(ulong);
            }
        }

        while (data.Length - offset >= sizeof(uint))
        {
            crc = ArmCrc32.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt32LittleEndian(data[offset..]));
            offset += sizeof(uint);
        }

        if (data.Length - offset >= sizeof(ushort))
        {
            crc = ArmCrc32.ComputeCrc32C(crc, BinaryPrimitives.ReadUInt16LittleEndian(data[offset..]));
            offset += sizeof(ushort);
        }

        if (offset < data.Length)
        {
            crc = ArmCrc32.ComputeCrc32C(crc, data[offset]);
        }

        return ~crc;
    }
}
