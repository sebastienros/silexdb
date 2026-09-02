using Microsoft.CodeAnalysis;

namespace Silex.Analyzers;

internal static class OwnershipDiagnostics
{
    internal const string CopySensitiveId = "SILEX001";
    internal const string UndisposedResultId = "SILEX002";
    internal const string IncompleteDisposeId = "SILEX003";

    internal static readonly DiagnosticDescriptor CopySensitive = new(
        CopySensitiveId,
        "Copy-sensitive value is copied",
        "Copy-sensitive value '{0}' is copied by value; use a ref flow or an explicit ownership transfer",
        "Ownership",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Copying a marked disposable struct creates multiple values that can release the same ownership token.");

    internal static readonly DiagnosticDescriptor UndisposedResult = new(
        UndisposedResultId,
        "Owned result is not disposed",
        "Owned result '{0}' must be disposed or explicitly transferred",
        "Ownership",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "A directly acquired marked resource must use a using declaration, be disposed in the scope, or leave through an explicit ownership flow.");

    internal static readonly DiagnosticDescriptor IncompleteDispose = new(
        IncompleteDisposeId,
        "Dispose does not release an owned member",
        "Dispose path for '{0}' does not release owned member '{1}'",
        "Ownership",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Every member marked as owned must be released by Dispose, either directly or through a same-type helper method.",
        customTags: WellKnownDiagnosticTags.CompilationEnd);
}
