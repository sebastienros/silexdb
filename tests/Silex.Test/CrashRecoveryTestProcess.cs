using System.Diagnostics;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Silex.Test;

internal static class CrashRecoveryTestProcess
{
    private const string StoragePathVariable = "SILEX_TEST_CRASH_STORAGE_PATH";
    private const string EntryCountVariable = "SILEX_TEST_CRASH_ENTRY_COUNT";
    private const string BatchVariable = "SILEX_TEST_CRASH_BATCH";
    private const string FaultPointVariable = "SILEX_TEST_CRASH_POINT";
    private const string FaultModeVariable = "SILEX_TEST_CRASH_MODE";

    // A child starts this test assembly and exits here before TUnit runs, without disposing the store.
    [ModuleInitializer]
    internal static void RunIfRequested()
    {
        var storagePath = Environment.GetEnvironmentVariable(StoragePathVariable);
        if (storagePath is null)
        {
            return;
        }

        if (Environment.GetEnvironmentVariable(FaultPointVariable) is { } faultPoint)
        {
            RunFaultCrash(storagePath, (StorageFaultPoint)int.Parse(faultPoint, CultureInfo.InvariantCulture),
                Environment.GetEnvironmentVariable(FaultModeVariable)!);
            return;
        }

        var entryCount = int.Parse(
            Environment.GetEnvironmentVariable(EntryCountVariable)!,
            CultureInfo.InvariantCulture);
        if (Environment.GetEnvironmentVariable(BatchVariable) == "1")
        {
            var storage = LsmStorage.OpenAsync(
                    storagePath,
                    new StorageOptions { FlushPeriod = TimeSpan.Zero })
                .GetAwaiter()
                .GetResult();
            var batch = new LsmWriteBatch();

            Span<byte> key = stackalloc byte[sizeof(int)];
            Span<byte> value = stackalloc byte[sizeof(int)];
            for (var i = 0; i < entryCount; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(key, i);
                BinaryPrimitives.WriteInt32LittleEndian(value, i + 1);
                batch.Put(key, value);
            }

            storage.Write(batch);
            GC.KeepAlive(storage);
        }
        else
        {
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
        }

        Environment.Exit(0);
    }

    public static async Task WriteAndExitWithoutDisposalAsync(string storagePath, int entryCount)
    {
        await WriteAndExitWithoutDisposalAsync(storagePath, entryCount, useBatch: false);
    }

    public static async Task WriteBatchAndExitWithoutDisposalAsync(string storagePath, int entryCount)
    {
        await WriteAndExitWithoutDisposalAsync(storagePath, entryCount, useBatch: true);
    }

    public static Task CrashAtAsync(string storagePath, int point, string mode = "write") =>
        WriteAndExitWithoutDisposalAsync(storagePath, 2, true, point, mode);

    private static void RunFaultCrash(string path, StorageFaultPoint point, string mode)
    {
        var options = mode is "tiered" or "leveled"
            ? StorageFailureTests.MaintenanceOptions(mode == "leveled")
            : new StorageOptions { FlushPeriod = TimeSpan.Zero, SyncWriteAheadLogToDisk = true };
        var armed = mode != "restore";
        options.FaultInjector = p =>
        {
            if (armed && p == point)
            {
                Environment.Exit(0);
            }
        };
        if (mode == "mvcc")
        {
            var mvcc = MvccStorage.OpenAsync(path, options).GetAwaiter().GetResult();
            mvcc.Put([1], [11]);
            GC.KeepAlive(mvcc);
        }
        else
        {
            var storage = LsmStorage.OpenAsync(path, options).GetAwaiter().GetResult();
            if (mode is "write" or "flush" or "synchronize" or "backup" or "restore")
            {
                var batch = new LsmWriteBatch();
                batch.Put([1], [11]);
                batch.Put([2], [22]);
                storage.Write(batch);
            }
            if (mode is "flush" or "tiered" or "leveled")
            {
                storage.FlushAndCompactAsync().GetAwaiter().GetResult();
            }
            if (mode == "synchronize")
            {
                storage.SynchronizeAsync().GetAwaiter().GetResult();
            }
            if (mode is "backup" or "restore")
            {
                storage.CreateBackupAsync(path + "-backup").GetAwaiter().GetResult();
                if (mode == "restore")
                {
                    armed = true;
                    LsmStorage.RestoreBackupAsync(path + "-backup", path + "-restored", options).GetAwaiter().GetResult();
                }
            }
            GC.KeepAlive(storage);
        }
        // Reaching here means the requested fault point was not exercised.
        Environment.Exit(3);
    }

    private static async Task WriteAndExitWithoutDisposalAsync(string storagePath, int entryCount, bool useBatch,
        int? faultPoint = null, string? faultMode = null)
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
        if (useBatch)
        {
            startInfo.Environment[BatchVariable] = "1";
        }
        if (faultPoint is { } point)
        {
            startInfo.Environment[FaultPointVariable] = point.ToString(CultureInfo.InvariantCulture);
            startInfo.Environment[FaultModeVariable] = faultMode;
        }

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
