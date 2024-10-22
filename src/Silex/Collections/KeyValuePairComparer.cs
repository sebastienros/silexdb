namespace Silex.Collections;

public sealed class KeyValuePairComparer<TKey, TValue> : Comparer<KeyValuePair<TKey, TValue>>
{
    internal IComparer<TKey> keyComparer;

    public KeyValuePairComparer(IComparer<TKey>? keyComparer)
    {
        this.keyComparer = keyComparer ?? Comparer<TKey>.Default;
    }

    public override int Compare(KeyValuePair<TKey, TValue> x, KeyValuePair<TKey, TValue> y)
    {
        return keyComparer.Compare(x.Key, y.Key);
    }

    public override bool Equals(object? obj)
    {
        if (obj is KeyValuePairComparer<TKey, TValue> other)
        {
            // Commonly, both comparers will be the default comparer (and reference-equal). Avoid a virtual method call to Equals() in that case.
            return this.keyComparer == other.keyComparer || this.keyComparer.Equals(other.keyComparer);
        }
        return false;
    }

    public override int GetHashCode()
    {
        return this.keyComparer.GetHashCode();
    }
}
