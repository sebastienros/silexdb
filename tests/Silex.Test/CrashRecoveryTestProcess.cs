using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Silex.Test;

internal static class CrashRecoveryTestProcess
{
    private const string StoragePathVariable = "SILEX_TEST_CRASH_STORAGE_PATH";
    private const string EntryCountVariable = "SILEX_TEST_CRASH_ENTRY_COUNT";

    // A child starts this test assembly and exits here before TUnit runs, without disposing the store.
    [ModuleInitializer]
    internal static void RunIfRequested()
    {
        var storagePath = Environment.GetEnvironmentVariable(StoragePathVariable);
        if (storagePath is null)
        {
            return;
        }

        var entryCount = int.Parse(
            Environment.GetEnvironmentVariable(EntryCountVariable)!,
            CultureInfo.InvariantCulture);
        var storage = LsmStorage.OpenAsync<int, int>(
                storagePath,
                new StorageOptions { FlushPeriod = TimeSpan.Zero })
            .GetAwaiter()
            .GetResult();

        for (var i = 0; i < entryCount; i++)
        {
            storage.Put(i, i + 1);
        }

        GC.KeepAlive(storage);
        Environment.Exit(0);
    }

    public static async Task WriteAndExitWithoutDisposalAsync(string storagePath, int entryCount)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        startInfo.ArgumentList.Add(System.Reflection.Assembly.GetExecutingAssembly().Location);
        startInfo.Environment[StoragePathVariable] = storagePath;
        startInfo.Environment[EntryCountVariable] = entryCount.ToString(CultureInfo.InvariantCulture);

        using var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start the WAL crash-test process.");
        var standardOutput = process.StandardOutput.ReadToEndAsync();
        var standardError = process.StandardError.ReadToEndAsync();

        await process.WaitForExitAsync();
        var output = await standardOutput;
        var error = await standardError;

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"WAL crash-test process exited with code {process.ExitCode}.{Environment.NewLine}" +
                output + error);
        }
    }
}
