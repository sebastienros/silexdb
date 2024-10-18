namespace Silex;

public static class DurationExtensions
{
    /// <summary>
    /// Minutes.
    /// </summary>
    public static TimeSpan Day(this int @this)
    {
        return TimeSpan.FromDays(@this);
    }

    /// <summary>
    /// Minutes.
    /// </summary>
    public static TimeSpan Hour(this int @this)
    {
        return TimeSpan.FromHours(@this);
    }

    /// <summary>
    /// Minutes.
    /// </summary>
    public static TimeSpan Minute(this int @this)
    {
        return TimeSpan.FromMinutes(@this);
    }

    /// <summary>
    /// Minutes.
    /// </summary>
    public static TimeSpan Second(this int @this)
    {
        return TimeSpan.FromSeconds(@this);
    }
}
