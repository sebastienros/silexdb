namespace Silex;

internal class IdGenerator
{
    private static long _twentyTwentyFour = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static long _lastId = DateTime.UtcNow.Ticks - _twentyTwentyFour;

    public static long GetNextId() => Interlocked.Increment(ref _lastId);

    /// <summary>
    /// Ensures subsequent ids returned by <see cref="GetNextId"/> are greater than <paramref name="id"/>.
    /// Used on open so freshly generated ids never collide with one already persisted on disk.
    /// </summary>
    public static void EnsureGreaterThan(long id)
    {
        long current;
        do
        {
            current = Interlocked.Read(ref _lastId);
            if (current >= id)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(ref _lastId, id, current) != current);
    }
}
