using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Silex.Analyzers;

namespace Silex.Analyzers.Tests;

public sealed class OwnershipAnalyzerTests
{
    private const string Markers = """
        namespace Silex.Ownership
        {
            [System.AttributeUsage(System.AttributeTargets.Struct)]
            internal sealed class CopySensitiveAttribute : System.Attribute { }

            [System.AttributeUsage(System.AttributeTargets.Class | System.AttributeTargets.Struct)]
            internal sealed class MustDisposeAttribute : System.Attribute { }

            [System.AttributeUsage(System.AttributeTargets.Parameter)]
            internal sealed class ConsumesOwnershipAttribute : System.Attribute { }

            [System.AttributeUsage(System.AttributeTargets.Field | System.AttributeTargets.Property)]
            internal sealed class OwnedResourceAttribute : System.Attribute { }
        }
        """;

    [Test]
    public async Task ReportsLocalCopyAndByValueArgument()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [CopySensitive]
            struct Lease : System.IDisposable { public void Dispose() { } }
            class C
            {
                void M()
                {
                    using var first = new Lease();
                    using var second = first;
                    Consume(first);
                }
                void Consume(Lease lease) { }
            }
            """);

        AssertIds(diagnostics, "SILEX001", "SILEX001");
    }

    [Test]
    public async Task AllowsConstructionUsingRefAndExplicitTransferFlows()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [CopySensitive, MustDispose]
            struct Lease : System.IDisposable { public void Dispose() { } }
            class C
            {
                void M()
                {
                    using var lease = new Lease();
                    Inspect(in lease);
                    Transfer(lease);
                }
                void Inspect(in Lease lease) { }
                void Transfer([ConsumesOwnership] Lease lease) => lease.Dispose();
            }
            """);

        AssertIds(diagnostics);
    }

    [Test]
    public async Task ReportsCopyReturnedFromStoredValue()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [CopySensitive]
            struct Lease : System.IDisposable { public void Dispose() { } }
            class C
            {
                private Lease lease;
                Lease GetLease() => lease;
            }
            """);

        AssertIds(diagnostics, "SILEX001");
    }

    [Test]
    public async Task ReportsUndisposedLocalAndDiscardedResult()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [MustDispose]
            sealed class Owner : System.IDisposable
            {
                public static Owner Create() => new Owner();
                public void Use() { }
                public void Dispose() { }
            }
            class C
            {
                void M()
                {
                    var owner = Owner.Create();
                    owner.Use();
                    Owner.Create();
                    _ = Owner.Create();
                }
            }
            """);

        AssertIds(diagnostics, "SILEX002", "SILEX002", "SILEX002");
    }

    [Test]
    public async Task AllowsUsingDisposeReturnAndMemberTransfer()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [MustDispose]
            sealed class Owner : System.IDisposable
            {
                public static Owner Create() => new Owner();
                public void Dispose() { }
            }
            class C
            {
                private Owner field = null!;
                Owner Return() { var owner = Owner.Create(); return owner; }
                void Store() { var owner = Owner.Create(); field = owner; }
                void Using() { using var owner = Owner.Create(); }
                void Explicit() { var owner = Owner.Create(); owner.Dispose(); }
            }
            """);

        AssertIds(diagnostics);
    }

    [Test]
    public async Task DoesNotTreatUnrelatedUsingOrConditionalDisposeAsComplete()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [MustDispose]
            sealed class Owner : System.IDisposable
            {
                public static Owner Create() => new Owner();
                public void Dispose() { }
            }
            class C
            {
                void M(bool condition)
                {
                    using (var other = Owner.Create())
                    {
                        var leaked = Owner.Create();
                    }

                    var conditional = Owner.Create();
                    if (condition)
                    {
                        conditional.Dispose();
                    }
                }
            }
            """);

        AssertIds(diagnostics, "SILEX002", "SILEX002");
    }

    [Test]
    public async Task ChecksAwaitedOwnedStructResults()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            [CopySensitive, MustDispose]
            struct BlockLease : System.IDisposable { public void Dispose() { } }
            class C
            {
                static async System.Threading.Tasks.ValueTask<BlockLease> AcquireAsync()
                {
                    await System.Threading.Tasks.Task.Yield();
                    return new BlockLease();
                }
                async System.Threading.Tasks.Task LeakAsync()
                {
                    var lease = await AcquireAsync();
                }
                async System.Threading.Tasks.Task SafeAsync()
                {
                    using var lease = await AcquireAsync();
                }
            }
            """);

        AssertIds(diagnostics, "SILEX002");
    }

    [Test]
    public async Task ReportsOwnedMemberMissingFromDispose()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            sealed class Owner : System.IDisposable { public void Dispose() { } }
            sealed class Container : System.IDisposable
            {
                [OwnedResource] private readonly Owner owner = new Owner();
                public void Dispose() { }
            }
            """);

        AssertIds(diagnostics, "SILEX003");
    }

    [Test]
    public async Task FollowsSameTypeDisposeHelpers()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            sealed class Owner : System.IDisposable { public void Dispose() { } }
            sealed class Container : System.IDisposable
            {
                [OwnedResource] private readonly Owner owner = new Owner();
                public void Dispose() => DisposeCore();
                private void DisposeCore() => owner.Dispose();
            }
            """);

        AssertIds(diagnostics);
    }

    [Test]
    public async Task DoesNotFollowDisposeHelperOnAnotherInstance()
    {
        var diagnostics = await AnalyzeAsync("""
            using Silex.Ownership;
            sealed class Owner : System.IDisposable { public void Dispose() { } }
            sealed class Container : System.IDisposable
            {
                [OwnedResource] private readonly Owner owner = new Owner();
                private readonly Container other = null!;
                public void Dispose() => other.DisposeCore();
                private void DisposeCore() => owner.Dispose();
            }
            """);

        AssertIds(diagnostics, "SILEX003");
    }

    private static async Task<ImmutableArray<Diagnostic>> AnalyzeAsync(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(
            source + Environment.NewLine + Markers,
            new CSharpParseOptions(LanguageVersion.Latest));
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Select(static path => MetadataReference.CreateFromFile(path));
        var compilation = CSharpCompilation.Create(
            "AnalyzerTest",
            [syntaxTree],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var compilerErrors = compilation.GetDiagnostics()
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();
        if (compilerErrors.Length != 0)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, compilerErrors.AsEnumerable()));
        }

        return await compilation
            .WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(new OwnershipAnalyzer()))
            .GetAnalyzerDiagnosticsAsync();
    }

    private static void AssertIds(ImmutableArray<Diagnostic> diagnostics, params string[] expected)
    {
        var actual = diagnostics
            .OrderBy(static diagnostic => diagnostic.Location.SourceSpan.Start)
            .Select(static diagnostic => diagnostic.Id)
            .ToArray();
        if (!actual.SequenceEqual(expected))
        {
            throw new InvalidOperationException(
                $"Expected [{string.Join(", ", expected)}], actual [{string.Join(", ", actual)}].{Environment.NewLine}"
                + string.Join(Environment.NewLine, diagnostics.AsEnumerable()));
        }
    }
}
