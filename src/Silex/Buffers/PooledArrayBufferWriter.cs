// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Buffers;
using System.Diagnostics;

namespace Silex.Buffers;

/// <summary>
/// Provides a way to write to an automatically growing buffer backed by the array pool to prevent allocations.
/// The first buffer can be initialized with <see cref="PooledArrayBufferWriter(int)"/>. A default size will be used otherwise.
/// The buffer will double every time it's too small to fit more data. The final buffer can be retrieved using <see cref="WrittenMemory"/>.
/// The final buffer shouldn't be used once the <see cref="PooledArrayBufferWriter<T>"/> is disposed since it will be returned to the pool.
/// </summary>
/// <typeparam name="T"></typeparam>

// Copied from https://github.com/dotnet/aspnetcore/blob/d26729ef12e126cb15ce65d15bdef7fcf31c4e60/src/Shared/PooledArrayBufferWriter.cs
internal sealed class PooledArrayBufferWriter<T> : IBufferWriter<T>, IDisposable
{
    private T[] _rentedBuffer;
    private int _index;

    private const int MinimumBufferSize = 256;

    public PooledArrayBufferWriter()
    {
        _rentedBuffer = ArrayPool<T>.Shared.Rent(MinimumBufferSize);
        _index = 0;
    }

    public PooledArrayBufferWriter(int initialCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(initialCapacity);

        _rentedBuffer = ArrayPool<T>.Shared.Rent(initialCapacity);
        _index = 0;
    }

    /// <summary>
    /// Gets a <see cref="ReadOnlyMemory{T}"/> representing the written content.
    /// </summary>
    public ReadOnlyMemory<T> WrittenMemory
    {
        get
        {
            CheckIfDisposed();

            return _rentedBuffer.AsMemory(0, _index);
        }
    }

    /// <summary>
    /// Gets the number of elements written to the buffer.
    /// </summary>
    public int WrittenCount
    {
        get
        {
            CheckIfDisposed();

            return _index;
        }
    }

    /// <summary>
    /// Gets the capacity of the internal buffer.
    /// </summary>
    public int Capacity
    {
        get
        {
            CheckIfDisposed();

            return _rentedBuffer.Length;
        }
    }

    /// <summary>
    /// Gets the remaining capacity of the internal buffer.
    /// </summary>
    public int FreeCapacity
    {
        get
        {
            CheckIfDisposed();

            return _rentedBuffer.Length - _index;
        }
    }

    /// <summary>
    /// Clears the current buffer such that it can be used again without returning it to the pool
    /// </summary>
    public void Clear()
    {
        CheckIfDisposed();

        ClearHelper();
    }

    private void ClearHelper()
    {
        Debug.Assert(_rentedBuffer != null);

        _rentedBuffer.AsSpan(0, _index).Clear();
        _index = 0;
    }

    /// <summary>
    /// Returns the rented buffer back to the pool.
    /// </summary>
    public void Dispose()
    {
        if (_rentedBuffer == null)
        {
            return;
        }

        ClearHelper();
        ArrayPool<T>.Shared.Return(_rentedBuffer);
        _rentedBuffer = null!;
    }

    private void CheckIfDisposed()
    {
        if (_rentedBuffer == null)
        {
            ThrowObjectDisposedException();
        }
    }

    private static void ThrowObjectDisposedException()
    {
        throw new ObjectDisposedException(nameof(ArrayBufferWriter<T>));
    }

    public void Advance(int count)
    {
        CheckIfDisposed();

        ArgumentOutOfRangeException.ThrowIfNegative(count);

        if (_index > _rentedBuffer.Length - count)
        {
            ThrowInvalidOperationException(_rentedBuffer.Length);
        }

        _index += count;
    }

    /// <summary>
    /// Gets a <see cref="Memory{T}"/> that can be written to.
    /// </summary>
    public Memory<T> GetMemory(int sizeHint = 0)
    {
        CheckIfDisposed();

        CheckAndResizeBuffer(sizeHint);
        return _rentedBuffer.AsMemory(_index);
    }

    /// <summary>
    /// Gets a <see cref="Span{T}"/> that can be written to.
    /// </summary>
    public Span<T> GetSpan(int sizeHint = 0)
    {
        CheckIfDisposed();

        CheckAndResizeBuffer(sizeHint);
        return _rentedBuffer.AsSpan(_index);
    }

    private void CheckAndResizeBuffer(int sizeHint)
    {
        Debug.Assert(_rentedBuffer != null);

        ArgumentOutOfRangeException.ThrowIfNegative(sizeHint);

        if (sizeHint == 0)
        {
            sizeHint = MinimumBufferSize;
        }

        var availableSpace = _rentedBuffer.Length - _index;

        if (sizeHint > availableSpace)
        {
            var growBy = Math.Max(sizeHint, _rentedBuffer.Length);

            var newSize = checked(_rentedBuffer.Length + growBy);

            var oldBuffer = _rentedBuffer;

            _rentedBuffer = ArrayPool<T>.Shared.Rent(newSize);

            Debug.Assert(oldBuffer.Length >= _index);
            Debug.Assert(_rentedBuffer.Length >= _index);

            var previousBuffer = oldBuffer.AsSpan(0, _index);
            previousBuffer.CopyTo(_rentedBuffer);
            previousBuffer.Clear();
            ArrayPool<T>.Shared.Return(oldBuffer);
        }

        Debug.Assert(_rentedBuffer.Length - _index > 0);
        Debug.Assert(_rentedBuffer.Length - _index >= sizeHint);
    }

    private static void ThrowInvalidOperationException(int capacity)
    {
        throw new InvalidOperationException($"Cannot advance past the end of the buffer, which has a size of {capacity}.");
    }
}
