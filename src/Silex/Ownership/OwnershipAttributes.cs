namespace Silex.Ownership;

/// <summary>
/// Marks a disposable value type whose implicit copies would represent the same ownership token.
/// </summary>
[AttributeUsage(AttributeTargets.Struct)]
internal sealed class CopySensitiveAttribute : Attribute;

/// <summary>
/// Marks a type whose newly acquired values must be disposed or explicitly transferred.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
internal sealed class MustDisposeAttribute : Attribute;

/// <summary>
/// Marks a parameter that takes responsibility for disposing the supplied owned value.
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
internal sealed class ConsumesOwnershipAttribute : Attribute;

/// <summary>
/// Marks a field or property that the containing type's disposal path must release.
/// </summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
internal sealed class OwnedResourceAttribute : Attribute;
