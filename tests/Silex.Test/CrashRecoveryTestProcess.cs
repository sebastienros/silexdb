using System.Diagnostics;
using System.Globalization;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Silex.Test;

internal static class CrashRecoveryTestProcess
{
    private const string StoragePathVariable = "SILEX_TEST_CRASH_STORAGE_PATH";
    private const string EntryCountVariable = "SILEX_TEST_CRASH_ENTRY_COUNT";
    private const string BatchSizeVariable = "SILEX_TEST_CRASH_BATCH_SIZE";

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
        var batchSize = int.Parse(
            Environment.GetEnvironmentVariable(BatchSizeVariable) ?? "1",
            CultureInfo.InvariantCulture);

        if (batchSize > 1)
        {
            WriteBatchesAndExit(storagePath, entryCount, batchSize);
        }

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

    private static void WriteBatchesAndExit(string storagePath, int entryCount, int batchSize)
    {
        var storage = LsmStorage.OpenAsync(
                storagePath,
                new StorageOptions { FlushPeriod = TimeSpan.Zero })
            .GetAwaiter()
            .GetResult();
        var keys = new byte[batchSize * sizeof(int)];
        var values = new byte[batchSize * sizeof(int)];
        var entries = new WriteBatchEntry[batchSize];

        for (var start = 0; start < entryCount; start += batchSize)
        {
            var count = Math.Min(batchSize, entryCount - start);
            for (var i = 0; i < count; i++)
            {
                var key = keys.AsMemory(i * sizeof(int), sizeof(int));
                var value = values.AsMemory(i * sizeof(int), sizeof(int));
                BinaryPrimitives.WriteUInt32BigEndian(key.Span, (uint)(start + i) ^ 0x8000_0000u);
                BinaryPrimitives.WriteUInt32BigEndian(value.Span, (uint)(start + i + 1) ^ 0x8000_0000u);
                entries[i] = WriteBatchEntry.Put(key, value);
            }

            storage.WriteBatch(entries.AsSpan(0, count));
        }

        GC.KeepAlive(storage);
        Environment.Exit(0);
    }

    public static async Task WriteAndExitWithoutDisposalAsync(string storagePath, int entryCount, int batchSize = 1)
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
        startInfo.Environment[BatchSizeVariable] = batchSize.ToString(CultureInfo.InvariantCulture);

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
