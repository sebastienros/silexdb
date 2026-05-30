namespace Silex.Test;

internal sealed class TempFolder : IDisposable
{
    private readonly DirectoryInfo _directory;

    private TempFolder(DirectoryInfo directory)
    {
        _directory = directory;
    }

    public string Path => _directory.FullName;

    public static TempFolder Create() => new(Directory.CreateTempSubdirectory());

    public string GetRandomFileName() => System.IO.Path.Combine(Path, System.IO.Path.GetRandomFileName());

    public void Dispose()
    {
        if (Directory.Exists(Path))
        {
            Directory.Delete(Path, recursive: true);
        }
    }

    public override string ToString() => Path;

    public static implicit operator string(TempFolder folder) => folder.Path;
}
