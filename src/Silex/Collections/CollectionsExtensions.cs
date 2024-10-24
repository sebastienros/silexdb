using System.Reflection;
using System.Reflection.Emit;

namespace Silex.Collections;

internal static class CollectionsExtensions
{
    internal static int BinarySearch<T>(this IList<T> list, int index, int length, T value, IComparer<T> comparer)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(list.Count - index, length);

        int lo = index;
        int hi = index + length - 1;
        while (lo <= hi)
        {
            int i = lo + ((hi - lo) >> 1);
            int order = comparer.Compare(list[i], value);

            if (order == 0) return i;
            if (order < 0)
            {
                lo = i + 1;
            }
            else
            {
                hi = i - 1;
            }
        }

        return ~lo;
    }

    internal static IEnumerable<KeyValuePair<TKey, TValue>> Enumerate<TKey, TValue>(this SortedDictionary<TKey, TValue> dic, TKey from, TKey to, bool lowerBoundActive, bool upperBoundActive) where TKey : notnull
    {
        return InternalSortedSetTypeCache<TKey, TValue>.Enumerate(dic, from, to, lowerBoundActive, upperBoundActive);
    }

    /// <summary>
    /// We want to be able to iterate over a subset of the SortedDictionary. This is not possible with the public API.
    /// </summary>
    private static class InternalSortedSetTypeCache<TKey, TValue> where TKey : notnull
    {
        private static readonly Func<SortedDictionary<TKey, TValue>, SortedSet<KeyValuePair<TKey, TValue>>> _getSetValue;
        private static readonly Type _treeSubSetType;

        static InternalSortedSetTypeCache()
        {
            var sortedDictionaryType = typeof(SortedDictionary<TKey, TValue>);
            var sortedSetType = typeof(SortedSet<KeyValuePair<TKey, TValue>>);

            _treeSubSetType = sortedSetType.GetNestedType("TreeSubSet", BindingFlags.NonPublic)!.MakeGenericType(typeof(KeyValuePair<TKey, TValue>));

            // Create a delegate to access the private field _set
            // private readonly TreeSet<KeyValuePair<TKey, TValue>> _set
            var setField = sortedDictionaryType.GetField("_set", BindingFlags.NonPublic | BindingFlags.Instance)!;
            string methodName = setField.ReflectedType!.FullName + ".get_set" + setField.Name;
            DynamicMethod setterMethod = new DynamicMethod(methodName, typeof(SortedSet<KeyValuePair<TKey, TValue>>), [sortedDictionaryType], true);
            ILGenerator gen = setterMethod.GetILGenerator();
            gen.Emit(OpCodes.Ldarg_0);
            gen.Emit(OpCodes.Ldfld, setField);
            gen.Emit(OpCodes.Ret);
            _getSetValue = (Func<SortedDictionary<TKey, TValue>, SortedSet<KeyValuePair<TKey, TValue>>>)setterMethod.CreateDelegate(typeof(Func<SortedDictionary<TKey, TValue>, SortedSet<KeyValuePair<TKey, TValue>>>));
        }

        public static IEnumerable<KeyValuePair<TKey, TValue>> Enumerate(SortedDictionary<TKey, TValue> dic, TKey from, TKey to, bool lowerBoundActive, bool upperBoundActive)
        {
            var sortedSet = _getSetValue(dic);
            var treeSubSet = Activator.CreateInstance(_treeSubSetType, [sortedSet, new KeyValuePair<TKey, TValue>(from, default!), new KeyValuePair<TKey, TValue>(to, default!), lowerBoundActive, upperBoundActive])!;
            return (IEnumerable<KeyValuePair<TKey, TValue>>)treeSubSet;
        }
    }
}
