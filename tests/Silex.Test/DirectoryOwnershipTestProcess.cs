using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Silex.Test;

internal static class DirectoryOwnershipTestProcess
{
    private const string PathVariable = "SILEX_TEST_OWNERSHIP_PATH";
    private const string ModeVariable = "SILEX_TEST_OWNERSHIP_MODE";

    [ModuleInitializer]
    internal static void RunIfRequested()
    {
        if (Environment.GetEnvironmentVariable(PathVariable) is not { } path)
        {
            return;
        }

        try
        {
            var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };
            switch (Environment.GetEnvironmentVariable(ModeVariable))
            {
                case "integrity":
                    LsmStorage.VerifyIntegrityAsync(path, options).GetAwaiter().GetResult();
                    break;
                case "salvage":
                    using (LsmStorage.OpenSalvageAsync(path, options).GetAwaiter().GetResult()) { }
                    break;
                default:
                    using (LsmStorage.OpenAsync(path, options).GetAwaiter().GetResult()) { }
                    break;
            }
            Environment.Exit(3);
        }
        catch (IOException exception) when (exception.Message.Contains("Cannot exclusively own storage directory"))
        {
            Environment.Exit(0);
        }
    }

    internal static async Task AssertRejectedAsync(string path, string mode)
    {
        var start = new ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        start.ArgumentList.Add(System.Reflection.Assembly.GetExecutingAssembly().Location);
        start.Environment[PathVariable] = path;
        start.Environment[ModeVariable] = mode;
        using var process = Process.Start(start) ?? throw new InvalidOperationException("Cannot start ownership test process.");
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        try
        {
            await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(30));
            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"Ownership test process exited {process.ExitCode}: {await output}{await error}");
            }
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
        }
    }
}
