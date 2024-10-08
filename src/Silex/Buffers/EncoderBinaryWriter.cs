using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Silex.Buffers;

public ref struct EncoderBinaryWriter
{
    // this is effectively a cut-down re-implementation of BinaryWriter
    // from https://github.com/dotnet/runtime/blob/3689fbec921418e496962dc0ee252bdc9eafa3de/src/libraries/System.Private.CoreLib/src/System/IO/BinaryWriter.cs
    // and is byte-compatible; however, instead of working against a Stream, we work against a IBufferWriter<byte>
    //
    // note it also has APIs for writing raw BLOBs

    private readonly IBufferWriter<byte> _target;
    private int _offset; // position in the current buffer
    private int _length; // size of the current buffer
    private int _written; // number of bytes written if previous buffers
    private ref byte _root;

    public EncoderBinaryWriter(IBufferWriter<byte> target)
    {
        ArgumentNullException.ThrowIfNull(target);
        _target = target;
        _root = ref Unsafe.NullRef<byte>(); // no buffer initially
        _written = _offset = _length = 0;
        DebugAssertValid();
    }

    public readonly int BytesWritten => _written + _offset;

    private Span<byte> AvailableBuffer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            DebugAssertValid();
            return MemoryMarshal.CreateSpan(ref Unsafe.Add(ref _root, _offset), _length - _offset);
        }
    }

    [Conditional("DEBUG")]
    private void DebugAssertValid()
    {
        Debug.Assert(_target is not null);
        if (Unsafe.IsNullRef(ref _root))
        {
            // no buffer; expect all zeros
            Debug.Assert(_length == 0 && _offset == 0);
        }
        else
        {
            // have buffer; expect valid offset and positive length
            Debug.Assert(_offset >= 0 && _offset <= _length);
            Debug.Assert(_length > 0);
        }

    }

    // Writes a byte to this stream. The current position of the stream is
    // advanced by one.
    //
    public void Write(byte value)
    {
        if (_offset < _length)
        {
            Unsafe.Add(ref _root, _offset++) = value;
        }
        else
        {
            SlowWrite(value);
        }
        DebugAssertValid();
    }

    private void RequestNewBuffer()
    {
        _written += _offset;

        Flush();
        var span = _target.GetSpan(1024); // fairly arbitrary non-trivial buffer; we can explore larger if useful
        if (span.IsEmpty)
        {
            Throw();
        }
        _offset = 0;
        _length = span.Length;
        _root = ref MemoryMarshal.GetReference(span);

        DebugAssertValid();
        static void Throw() => throw new InvalidOperationException("Unable to acquire non-empty write buffer");
    }

    public void Flush() // commits the current buffer and leave in a buffer-free state
    {
        if (!Unsafe.IsNullRef(ref _root))
        {
            _target.Advance(_offset);
            _length = _offset = 0;
            _root = ref Unsafe.NullRef<byte>();
        }
        DebugAssertValid();
    }

    private void SlowWrite(byte value)
    {
        RequestNewBuffer();
        Unsafe.Add(ref _root, _offset++) = value;
    }

    public void Write7BitEncodedInt(int value)
    {
        uint uValue = (uint)value;

        // Write out an int 7 bits at a time. The high bit of the byte,
        // when on, tells reader to continue reading more bytes.
        //
        // Using the constants 0x7F and ~0x7F below offers smaller
        // codegen than using the constant 0x80.

        while (uValue > 0x7Fu)
        {
            Write((byte)(uValue | ~0x7Fu));
            uValue >>= 7;
        }

        Write((byte)uValue);
    }

    public void Write7BitEncodedInt64(long value)
    {
        ulong uValue = (ulong)value;

        // Write out an int 7 bits at a time. The high bit of the byte,
        // when on, tells reader to continue reading more bytes.
        //
        // Using the constants 0x7F and ~0x7F below offers smaller
        // codegen than using the constant 0x80.

        while (uValue > 0x7Fu)
        {
            Write((byte)((uint)uValue | ~0x7Fu));
            uValue >>= 7;
        }

        Write((byte)uValue);
    }

    public void WriteUInt16(ushort value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16LittleEndian(buffer, value);
        WriteRaw(buffer);
    }

    public void WriteUInt32(uint value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32LittleEndian(buffer, value);
        WriteRaw(buffer);
    }

    public void WriteUInt64(ulong value)
    {
        Span<byte> buffer = stackalloc byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(buffer, value);
        WriteRaw(buffer);
    }

    public void WriteRaw(scoped ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        { } // nothing to do
        else if (_offset + value.Length <= _length)
        {
            value.CopyTo(AvailableBuffer);
            _offset += value.Length;
        }
        else
        {
            SlowWriteRaw(value);
        }
        DebugAssertValid();
    }

    private void SlowWriteRaw(scoped ReadOnlySpan<byte> value)
    {
        do
        {
            RequestNewBuffer();
            var available = AvailableBuffer;
            var toWrite = Math.Min(value.Length, available.Length);
            value.Slice(start: 0, length: toWrite).CopyTo(available);
            _offset += toWrite;
            value = value.Slice(start: toWrite);
        }
        while (!value.IsEmpty);
    }
}
