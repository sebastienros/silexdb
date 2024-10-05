namespace Silex;

public static class SizeExtensions
{
    /// <summary>
    /// Gibibytes.
    /// </summary>
    public static long GiB(this long @this)
    {
        return @this * 1024L * 1024L * 1024L;
    }

    /// <summary>
    /// Mebibytes.
    /// </summary>
    public static long MiB(this long @this)
    {
        return @this * 1024L * 1024L;
    }

    /// <summary>
    /// Kibibytes.
    /// </summary>
    public static long KiB(this long @this)
    {
        return @this * 1024L;
    }

    /// <summary>
    /// Gibibytes.
    /// </summary>
    public static long GiB(this int @this)
    {
        return @this * 1024L * 1024L * 1024L;
    }

    /// <summary>
    /// Mebibytes.
    /// </summary>
    public static long MiB(this int @this)
    {
        return @this * 1024L * 1024L;
    }

    /// <summary>
    /// Kibibytes.
    /// </summary>
    public static long KiB(this int @this)
    {
        return @this * 1024L;
    }

    /// <summary>
    /// Bytes.
    /// </summary>
    public static long B(this int @this)
    {
        return @this;
    }
}
