using System.Text.Json;

namespace Silex.Tool;

internal sealed record ServerCredentials(string InstanceId, string Token)
{
    internal const string FileName = ".silex-server.json";

    public static ServerCredentials Read(string database)
    {
        var path = Path.Combine(database, FileName);
        var attributes = File.GetAttributes(path);
        if ((attributes & (FileAttributes.ReparsePoint | FileAttributes.Directory)) != 0)
            throw new IOException($"Server credentials must be a regular file, not a link: '{path}'.");
        using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var credentials = JsonSerializer.Deserialize(input, ToolJsonContext.Default.ServerCredentials);
        if (credentials is null || string.IsNullOrWhiteSpace(credentials.InstanceId) ||
            credentials.Token is not { Length: 64 } || !credentials.Token.All(char.IsAsciiHexDigit))
            throw new InvalidDataException($"Invalid server credentials in '{path}'.");
        return credentials;
    }

    public static void Write(string database, ServerCredentials credentials) =>
        Registry.WriteFile(Path.Combine(database, FileName), credentials, ToolJsonContext.Default.ServerCredentials, privateFile: false);

    public static void Remove(string database, string instanceId)
    {
        if (Read(database).InstanceId == instanceId)
            File.Delete(Path.Combine(database, FileName));
    }
}
