namespace Silex;

internal sealed class DirectoryOwnership : IDisposable
{
    internal const string FileName = "LOCK";
    private readonly FileStream _stream;

    private DirectoryOwnership(FileStream stream) => _stream = stream;

    internal static DirectoryOwnership Acquire(string path)
    {
        // Never unlink this file: a new inode would allow a second owner on Unix.
        var filename = Path.Combine(path, FileName);
        try
        {
            return new DirectoryOwnership(new FileStream(filename, FileMode.OpenOrCreate,
                FileAccess.Read, FileShare.None));
        }
        catch (IOException exception)
        {
            throw new IOException(
                $"Cannot exclusively own storage directory '{path}'. Another storage instance, salvage handle, or integrity check may own it. Close that owner before retrying; do not delete '{filename}'.",
                exception);
        }
    }

    public void Dispose() => _stream.Dispose();
}
