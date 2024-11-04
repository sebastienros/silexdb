namespace Silex;

internal class IdGenerator
{
    private static long _twentyTwentyFour = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private static long _lastId = DateTime.UtcNow.Ticks - _twentyTwentyFour;

    public static long GetNextId() => Interlocked.Increment(ref _lastId);
}
