namespace Silex;

/// <summary>
/// Callback invoked with a read-only borrow of an entry's raw value bytes for zero-copy inspection.
/// </summary>
/// <typeparam name="TArg">A caller-supplied state argument, passed through to avoid closure allocation.</typeparam>
/// <param name="arg">The caller-supplied state passed to the read call.</param>
/// <param name="value">
/// A read-only span over engine-owned value memory. It is only valid for the duration of the callback:
/// the callback must run synchronously and must not store the span, await, block, or call back into the
/// same store. Copy the bytes out if you need to keep them.
/// </param>
public delegate void ReadValueAction<in TArg>(TArg arg, ReadOnlySpan<byte> value);
