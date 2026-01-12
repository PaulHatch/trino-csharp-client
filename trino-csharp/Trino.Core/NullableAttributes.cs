#if NETSTANDARD2_0
// Polyfill for nullable attributes not present in .NET Standard 2.0
// ReSharper disable once CheckNamespace
namespace System.Diagnostics.CodeAnalysis;

[AttributeUsage(AttributeTargets.Field | AttributeTargets.Parameter | AttributeTargets.Property)]
internal sealed class AllowNullAttribute : Attribute;

[AttributeUsage(AttributeTargets.Field | AttributeTargets.Parameter | AttributeTargets.Property |
                AttributeTargets.ReturnValue)]
internal sealed class MaybeNullAttribute : Attribute;

[AttributeUsage(AttributeTargets.Field | AttributeTargets.Parameter | AttributeTargets.Property |
                AttributeTargets.ReturnValue)]
internal sealed class NotNullAttribute : Attribute;
#endif
