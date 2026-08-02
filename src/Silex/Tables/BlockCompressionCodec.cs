using K4os.Compression.LZ4;
using System.Buffers;
using ZstdSharp;

namespace Silex.Tables;

internal sealed class BlockCompressor : IDisposable
{
    private readonly SstCompression _compression;
    private readonly LZ4Level _lz4Level;
    private readonly Compressor? _zstdCompressor;
    private readonly double _minimumSavingsRatio;
    private byte[]? _buffer;

    public BlockCompressor(SstCompression compression, int compressionLevel, double minimumSavingsPercent)
    {
        if (!Enum.IsDefined(compression))
        {
            throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unsupported SST compression algorithm.");
        }

        if (minimumSavingsPercent is < 0 or >= 100)
        {
            throw new ArgumentOutOfRangeException(nameof(minimumSavingsPercent), minimumSavingsPercent, "Minimum compression savings must be between 0 (inclusive) and 100 (exclusive).");
        }

        _compression = compression;
        _minimumSavingsRatio = (100 - minimumSavingsPercent) / 100;

        if (compression == SstCompression.Lz4)
        {
            if (compressionLevel != 0 && compressionLevel is < 3 or > 12)
            {
                throw new ArgumentOutOfRangeException(nameof(compressionLevel), compressionLevel, "LZ4 compression level must be 0 (fast) or between 3 and 12.");
            }

            _lz4Level = (LZ4Level)compressionLevel;
        }
        else if (compression == SstCompression.Zstandard)
        {
            _zstdCompressor = compressionLevel == 0
                ? new Compressor()
                : new Compressor(compressionLevel);
        }
    }

    public CompressedBlock Compress(ReadOnlySpan<byte> source)
    {
        if (_compression == SstCompression.None
            || source.IsEmpty
            || source.Length > SsTableFormat.MaxCompressedBlockUncompressedLength)
        {
            return new CompressedBlock(source, SstCompression.None, source.Length);
        }

        var maximumLength = _compression switch
        {
            SstCompression.Lz4 => LZ4Codec.MaximumOutputSize(source.Length),
            SstCompression.Zstandard => Compressor.GetCompressBound(source.Length),
            _ => throw new InvalidOperationException($"Unsupported SST compression algorithm: {_compression}.")
        };

        EnsureBuffer(maximumLength);
        var destination = _buffer.AsSpan(0, maximumLength);

        var compressedLength = _compression switch
        {
            SstCompression.Lz4 => LZ4Codec.Encode(source, destination, _lz4Level),
            SstCompression.Zstandard => _zstdCompressor!.Wrap(source, destination),
            _ => -1,
        };

        if (compressedLength <= 0 || compressedLength > source.Length * _minimumSavingsRatio)
        {
            return new CompressedBlock(source, SstCompression.None, source.Length);
        }

        return new CompressedBlock(destination[..compressedLength], _compression, source.Length);
    }

    private void EnsureBuffer(int length)
    {
        if (_buffer is not null && _buffer.Length >= length)
        {
            return;
        }

        var replacement = ArrayPool<byte>.Shared.Rent(length);
        if (_buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }

        _buffer = replacement;
    }

    public void Dispose()
    {
        _zstdCompressor?.Dispose();
        if (_buffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = null;
        }
    }
}

internal static class BlockDecompressor
{
    [ThreadStatic]
    private static Decompressor? t_zstdDecompressor;

    public static void Decompress(SstCompression compression, ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int decodedLength;

        try
        {
            decodedLength = compression switch
            {
                SstCompression.Lz4 => LZ4Codec.Decode(source, destination),
                SstCompression.Zstandard => (t_zstdDecompressor ??= new Decompressor()).Unwrap(source, destination),
                _ => throw new InvalidDataException($"Unsupported SST block compression algorithm: {compression}."),
            };
        }
        catch (ZstdException exception)
        {
            throw new InvalidDataException("The SST contains an invalid Zstandard-compressed block.", exception);
        }

        if (decodedLength != destination.Length)
        {
            throw new InvalidDataException($"The SST block decompressed to {decodedLength} bytes instead of the expected {destination.Length} bytes.");
        }
    }
}

internal readonly ref struct CompressedBlock
{
    public CompressedBlock(ReadOnlySpan<byte> data, SstCompression compression, int uncompressedLength)
    {
        Data = data;
        Compression = compression;
        UncompressedLength = uncompressedLength;
    }

    public ReadOnlySpan<byte> Data { get; }
    public SstCompression Compression { get; }
    public int UncompressedLength { get; }
}
