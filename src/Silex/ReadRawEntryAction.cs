namespace Silex;

/// <summary>
/// Callback invoked with read-only borrows of an entry's encoded key and raw value bytes.
/// </summary>
/// <typeparam name="TArg">A caller-supplied state argument, passed through to avoid closure allocation.</typeparam>
/// <param name="arg">The caller-supplied state passed to the scan call.</param>
/// <param name="encodedKey">
/// A read-only span over the entry's on-disk encoded key bytes. For identity encoders such as
/// <see cref="byte"/>[] and <see cref="Bytes"/>, this is the original key byte sequence.
/// </param>
/// <param name="value">
/// A read-only span over engine-owned value memory. It is only valid for the duration of the callback:
/// the callback must run synchronously and must not store the span, await, block, or call back into the
/// same store. Copy the bytes out if you need to keep them.
/// </param>
/// <returns><c>true</c> to continue scanning; <c>false</c> to stop after this entry.</returns>
public delegate bool ReadRawEntryAction<in TArg>(TArg arg, ReadOnlySpan<byte> encodedKey, ReadOnlySpan<byte> value);
