using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Silex.Analyzers;

/// <summary>
/// Enforces the deliberately small ownership contract expressed by Silex's internal marker attributes.
/// The analysis is intentionally intraprocedural except for following private disposal helpers.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class OwnershipAnalyzer : DiagnosticAnalyzer
{
    private const string CopySensitiveAttribute = "Silex.Ownership.CopySensitiveAttribute";
    private const string MustDisposeAttribute = "Silex.Ownership.MustDisposeAttribute";
    private const string ConsumesOwnershipAttribute = "Silex.Ownership.ConsumesOwnershipAttribute";
    private const string OwnedResourceAttribute = "Silex.Ownership.OwnedResourceAttribute";

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(
            OwnershipDiagnostics.CopySensitive,
            OwnershipDiagnostics.UndisposedResult,
            OwnershipDiagnostics.IncompleteDispose);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(static startContext =>
        {
            if (startContext.Compilation.GetTypeByMetadataName(MustDisposeAttribute) is not null)
            {
                startContext.RegisterOperationBlockAction(AnalyzeOwnershipBlock);
                startContext.RegisterOperationAction(AnalyzeDiscardedResult, OperationKind.ExpressionStatement);
                startContext.RegisterOperationAction(AnalyzeDiscardAssignment, OperationKind.SimpleAssignment);
            }

            if (startContext.Compilation.GetTypeByMetadataName(CopySensitiveAttribute) is not null)
            {
                startContext.RegisterOperationAction(AnalyzeArgumentCopy, OperationKind.Argument);
                startContext.RegisterOperationAction(AnalyzeAssignmentCopy, OperationKind.SimpleAssignment);
                startContext.RegisterOperationAction(AnalyzeLocalCopy, OperationKind.VariableDeclarator);
                startContext.RegisterOperationAction(AnalyzeReturnCopy, OperationKind.Return);
            }

            if (startContext.Compilation.GetTypeByMetadataName(OwnedResourceAttribute) is not null)
            {
                var disposeAnalysis = new DisposeAnalysis();
                startContext.RegisterSymbolAction(disposeAnalysis.CollectOwnedMembers, SymbolKind.NamedType);
                startContext.RegisterOperationAction(disposeAnalysis.CollectDisposeFlow, OperationKind.Invocation);
                startContext.RegisterCompilationEndAction(disposeAnalysis.ReportDiagnostics);
            }
        });
    }

    private static void AnalyzeArgumentCopy(OperationAnalysisContext context)
    {
        var argument = (IArgumentOperation)context.Operation;
        var parameter = argument.Parameter;
        var source = Unwrap(argument.Value);

        if (parameter is null
            || parameter.RefKind != RefKind.None
            || HasAttribute(parameter, ConsumesOwnershipAttribute)
            || !IsMarked(source.Type, CopySensitiveAttribute)
            || !IsExistingValueReference(source)
            || IsValueTaskOwnershipWrapper(argument))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            OwnershipDiagnostics.CopySensitive,
            argument.Syntax.GetLocation(),
            source.Type!.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    private static void AnalyzeAssignmentCopy(OperationAnalysisContext context)
    {
        var assignment = (ISimpleAssignmentOperation)context.Operation;
        var source = Unwrap(assignment.Value);
        if (!IsMarked(source.Type, CopySensitiveAttribute) || !IsExistingValueReference(source))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            OwnershipDiagnostics.CopySensitive,
            assignment.Value.Syntax.GetLocation(),
            source.Type!.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    private static void AnalyzeLocalCopy(OperationAnalysisContext context)
    {
        var declarator = (IVariableDeclaratorOperation)context.Operation;
        if (declarator.Initializer is null)
        {
            return;
        }

        var source = Unwrap(declarator.Initializer.Value);
        if (!IsMarked(source.Type, CopySensitiveAttribute) || !IsExistingValueReference(source))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            OwnershipDiagnostics.CopySensitive,
            declarator.Initializer.Value.Syntax.GetLocation(),
            source.Type!.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    private static void AnalyzeReturnCopy(OperationAnalysisContext context)
    {
        var returnOperation = (IReturnOperation)context.Operation;
        if (returnOperation.ReturnedValue is null)
        {
            return;
        }

        var source = Unwrap(returnOperation.ReturnedValue);
        if (!IsMarked(source.Type, CopySensitiveAttribute)
            || source is not (IFieldReferenceOperation or IPropertyReferenceOperation or IArrayElementReferenceOperation))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            OwnershipDiagnostics.CopySensitive,
            returnOperation.ReturnedValue.Syntax.GetLocation(),
            source.Type!.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    private static void AnalyzeDiscardedResult(OperationAnalysisContext context)
    {
        var statement = (IExpressionStatementOperation)context.Operation;
        var expression = Unwrap(statement.Operation);
        if (!IsAcquisition(expression, out var acquiredType))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            OwnershipDiagnostics.UndisposedResult,
            expression.Syntax.GetLocation(),
            acquiredType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    private static void AnalyzeDiscardAssignment(OperationAnalysisContext context)
    {
        var assignment = (ISimpleAssignmentOperation)context.Operation;
        if (assignment.Target is not IDiscardOperation
            || !IsAcquisition(Unwrap(assignment.Value), out var acquiredType))
        {
            return;
        }

        context.ReportDiagnostic(Diagnostic.Create(
            OwnershipDiagnostics.UndisposedResult,
            assignment.Value.Syntax.GetLocation(),
            acquiredType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
    }

    private static void AnalyzeOwnershipBlock(OperationBlockAnalysisContext context)
    {
        foreach (var root in context.OperationBlocks)
        {
            foreach (var operation in DescendantsAndSelfInCurrentScope(root))
            {
                if (operation is not IVariableDeclaratorOperation { Initializer: not null } declarator
                    || !IsAcquisition(Unwrap(declarator.Initializer.Value), out var acquiredType)
                    || IsUsingDeclaration(declarator.Syntax)
                    || IsReleasedOrTransferred(root, declarator.Symbol, declarator.Syntax.SpanStart))
                {
                    continue;
                }

                context.ReportDiagnostic(Diagnostic.Create(
                    OwnershipDiagnostics.UndisposedResult,
                    declarator.Syntax.GetLocation(),
                    acquiredType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat)));
            }
        }
    }

    private static bool InvocationReceiverReferences(IInvocationOperation invocation, ISymbol member)
    {
        if (invocation.Instance is not null
            && ReferencesMember(Unwrap(invocation.Instance), member))
        {
            return true;
        }

        if (invocation.Parent is IConditionalAccessOperation conditional
            && ReferencesMember(Unwrap(conditional.Operation), member))
        {
            return true;
        }

        return false;
    }

    private static bool IsReleasedOrTransferred(IOperation root, ILocalSymbol local, int acquisitionStart)
    {
        foreach (var operation in DescendantsAndSelfInCurrentScope(root))
        {
            switch (operation)
            {
                case IInvocationOperation { TargetMethod.Name: "Dispose" or "DisposeAsync" } invocation
                    when ReceiverReferencesLocal(invocation, local)
                         && IsUnconditionalAfterAcquisition(root, invocation, acquisitionStart):
                case IReturnOperation returnOperation
                    when ContainsLocal(returnOperation.ReturnedValue, local):
                case ISimpleAssignmentOperation assignment
                    when assignment.Target is IFieldReferenceOperation or IPropertyReferenceOperation
                         && ContainsLocal(assignment.Value, local)
                         && IsUnconditionalAfterAcquisition(root, assignment, acquisitionStart):
                    return true;
                case IArgumentOperation argument
                    when HasAttribute(argument.Parameter, ConsumesOwnershipAttribute)
                         && ContainsLocal(argument.Value, local)
                         && IsUnconditionalAfterAcquisition(root, argument, acquisitionStart):
                    return true;
            }
        }

        return false;
    }

    private static bool ReceiverReferencesLocal(IInvocationOperation invocation, ILocalSymbol local)
    {
        if (invocation.Instance is not null && ContainsLocal(invocation.Instance, local))
        {
            return true;
        }

        return invocation.Parent is IConditionalAccessOperation conditional
            && ContainsLocal(conditional.Operation, local);
    }

    private static bool ContainsLocal(IOperation? operation, ILocalSymbol local) =>
        operation is not null
        && DescendantsAndSelfInCurrentScope(operation).Any(candidate =>
            candidate is ILocalReferenceOperation reference
            && SymbolEqualityComparer.Default.Equals(reference.Local, local));

    private static bool IsUnconditionalAfterAcquisition(IOperation root, IOperation release, int acquisitionStart)
    {
        if (release.Syntax.AncestorsAndSelf().OfType<Microsoft.CodeAnalysis.CSharp.Syntax.FinallyClauseSyntax>().Any())
        {
            return true;
        }

        for (var parent = release.Parent; parent is not null && parent != root; parent = parent.Parent)
        {
            if (parent is IConditionalOperation
                or IAnonymousFunctionOperation
                or ILocalFunctionOperation)
            {
                return false;
            }
        }

        return !DescendantsAndSelfInCurrentScope(root).OfType<IReturnOperation>().Any(returnOperation =>
            returnOperation.Syntax.SpanStart > acquisitionStart
            && returnOperation.Syntax.SpanStart < release.Syntax.SpanStart);
    }

    private static bool IsAcquisition(IOperation operation, out ITypeSymbol acquiredType)
    {
        acquiredType = operation.Type!;
        return acquiredType is not null
            && IsMarked(acquiredType, MustDisposeAttribute)
            && operation is IInvocationOperation or IObjectCreationOperation or IAwaitOperation;
    }

    private static bool IsUsingDeclaration(SyntaxNode syntax) =>
        syntax.AncestorsAndSelf().OfType<LocalDeclarationStatementSyntax>()
            .Any(static declaration => !declaration.UsingKeyword.IsKind(Microsoft.CodeAnalysis.CSharp.SyntaxKind.None))
        || syntax.AncestorsAndSelf().OfType<UsingStatementSyntax>()
            .Any(usingStatement => usingStatement.Declaration?.Span.Contains(syntax.Span) == true);

    private static bool IsExistingValueReference(IOperation operation) =>
        operation is ILocalReferenceOperation
            or IParameterReferenceOperation
            or IFieldReferenceOperation
            or IPropertyReferenceOperation
            or IArrayElementReferenceOperation;

    private static bool IsValueTaskOwnershipWrapper(IArgumentOperation argument)
    {
        if (argument.Parent is not IObjectCreationOperation creation)
        {
            return false;
        }

        var containingType = creation.Constructor?.ContainingType;
        return containingType is { Name: "ValueTask", ContainingNamespace: { } containingNamespace }
            && containingNamespace.ToDisplayString() == "System.Threading.Tasks"
            && containingType.IsGenericType;
    }

    private static IOperation Unwrap(IOperation operation)
    {
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }

        return operation;
    }

    private static bool ReferencesMember(IOperation operation, ISymbol member) =>
        operation switch
        {
            IFieldReferenceOperation field => SymbolEqualityComparer.Default.Equals(field.Field, member),
            IPropertyReferenceOperation property => SymbolEqualityComparer.Default.Equals(property.Property, member),
            _ => false
        };

    private static bool IsMarked(ITypeSymbol? type, string attributeName)
    {
        type = type is INamedTypeSymbol { OriginalDefinition.SpecialType: SpecialType.System_Nullable_T } nullable
            ? nullable.TypeArguments[0]
            : type;
        return type is not null && HasAttribute(type, attributeName);
    }

    private static bool HasAttribute(ISymbol? symbol, string attributeName) =>
        symbol is not null
        && symbol.GetAttributes().Any(attribute =>
            attribute.AttributeClass?.ToDisplayString() == attributeName);

    private static IEnumerable<IOperation> DescendantsAndSelf(IOperation operation)
    {
        yield return operation;
        foreach (var child in operation.ChildOperations)
        {
            foreach (var descendant in DescendantsAndSelf(child))
            {
                yield return descendant;
            }
        }
    }

    private static IEnumerable<IOperation> DescendantsAndSelfInCurrentScope(IOperation operation)
    {
        yield return operation;
        foreach (var child in operation.ChildOperations)
        {
            if (child is IAnonymousFunctionOperation or ILocalFunctionOperation)
            {
                continue;
            }

            foreach (var descendant in DescendantsAndSelfInCurrentScope(child))
            {
                yield return descendant;
            }
        }
    }

    private sealed class DisposeAnalysis
    {
        private readonly ConcurrentDictionary<ISymbol, byte> _ownedMembers =
            new(SymbolEqualityComparer.Default);
        private readonly ConcurrentDictionary<IMethodSymbol, ConcurrentDictionary<ISymbol, byte>> _releasedMembers =
            new(SymbolEqualityComparer.Default);
        private readonly ConcurrentDictionary<IMethodSymbol, ConcurrentDictionary<IMethodSymbol, byte>> _calls =
            new(SymbolEqualityComparer.Default);

        internal void CollectOwnedMembers(SymbolAnalysisContext context)
        {
            var type = (INamedTypeSymbol)context.Symbol;
            foreach (var member in type.GetMembers())
            {
                if (member is IFieldSymbol or IPropertySymbol
                    && HasAttribute(member, OwnedResourceAttribute))
                {
                    _ownedMembers.TryAdd(member, 0);
                }
            }
        }

        internal void CollectDisposeFlow(OperationAnalysisContext context)
        {
            if (context.ContainingSymbol is not IMethodSymbol containingMethod)
            {
                return;
            }

            var invocation = (IInvocationOperation)context.Operation;
            if (invocation.TargetMethod.Name is "Dispose" or "DisposeAsync")
            {
                var receiver = invocation.Parent is IConditionalAccessOperation conditional
                    ? conditional.Operation
                    : invocation.Instance;
                if (receiver is not null)
                {
                    var member = Unwrap(receiver) switch
                    {
                        IFieldReferenceOperation field => (ISymbol)field.Field,
                        IPropertyReferenceOperation property => property.Property,
                        _ => null
                    };
                    if (member is not null && HasAttribute(member, OwnedResourceAttribute))
                    {
                        GetReleasedSet(containingMethod).TryAdd(member, 0);
                    }
                }
            }

            var target = invocation.TargetMethod.OriginalDefinition;
            var isCurrentInstance = invocation.Instance is null
                || invocation.Instance is IInstanceReferenceOperation
                {
                    ReferenceKind: InstanceReferenceKind.ContainingTypeInstance
                };
            if (isCurrentInstance
                && SymbolEqualityComparer.Default.Equals(target.ContainingType, containingMethod.ContainingType))
            {
                GetCallSet(containingMethod).TryAdd(target, 0);
            }
        }

        internal void ReportDiagnostics(CompilationAnalysisContext context)
        {
            foreach (var member in _ownedMembers.Keys
                .OrderBy(static member => member.Locations.FirstOrDefault()?.SourceTree?.FilePath, StringComparer.Ordinal)
                .ThenBy(static member => member.Locations.FirstOrDefault()?.SourceSpan.Start)
                .ThenBy(static member => member.ToDisplayString(), StringComparer.Ordinal))
            {
                var type = member.ContainingType;
                var disposeMethods = type.GetMembers()
                    .OfType<IMethodSymbol>()
                    .Where(static method =>
                        !method.IsStatic
                        && method.Parameters.IsEmpty
                        && method.Name is "Dispose" or "DisposeAsync");
                if (disposeMethods.Any(method =>
                    ReleasesMember(method, member, new HashSet<IMethodSymbol>(SymbolEqualityComparer.Default))))
                {
                    continue;
                }

                context.ReportDiagnostic(Diagnostic.Create(
                    OwnershipDiagnostics.IncompleteDispose,
                    member.Locations.FirstOrDefault(),
                    type.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
                    member.Name));
            }
        }

        private bool ReleasesMember(IMethodSymbol method, ISymbol member, HashSet<IMethodSymbol> visited)
        {
            if (!visited.Add(method))
            {
                return false;
            }

            if (_releasedMembers.TryGetValue(method, out var released)
                && released.ContainsKey(member))
            {
                return true;
            }

            return _calls.TryGetValue(method, out var called)
                && called.Keys.Any(callee => ReleasesMember(callee, member, visited));
        }

        private ConcurrentDictionary<ISymbol, byte> GetReleasedSet(IMethodSymbol method) =>
            _releasedMembers.GetOrAdd(
                method,
                static _ => new ConcurrentDictionary<ISymbol, byte>(SymbolEqualityComparer.Default));

        private ConcurrentDictionary<IMethodSymbol, byte> GetCallSet(IMethodSymbol method) =>
            _calls.GetOrAdd(
                method,
                static _ => new ConcurrentDictionary<IMethodSymbol, byte>(SymbolEqualityComparer.Default));
    }
}
