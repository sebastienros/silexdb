using System.CommandLine;
using Silex.DbBench;

var root = CommandLine.BuildRootCommand(RunAsync);
return await root.Parse(args).InvokeAsync();

static async Task<int> RunAsync(BenchmarkOptions options, List<string> warnings)
{
    foreach (var warning in warnings)
    {
        Console.Error.WriteLine($"warning: {warning}");
    }

    if (options.KeySize < 1)
    {
        Console.Error.WriteLine("error: --key_size must be at least 1.");
        return 1;
    }

    // readmissing reads from [num, 2*num); make sure that range still fits in the key's numeric prefix so
    // miss keys cannot collide with written keys when key_size < 8.
    var prefixBits = Math.Min(8, options.KeySize) * 8;
    if (prefixBits < 64 && 2.0 * options.Num >= Math.Pow(2, prefixBits))
    {
        Console.Error.WriteLine($"error: --key_size {options.KeySize} is too small to hold 2*num={2 * options.Num} distinct keys.");
        return 1;
    }

    var dbPath = string.IsNullOrEmpty(options.Db)
        ? Path.Combine(Path.GetTempPath(), "silex-db-bench", Guid.NewGuid().ToString("N"))
        : options.Db;

    var ownsTempDir = string.IsNullOrEmpty(options.Db);

    try
    {
        await new Runner(options, dbPath).RunAsync();
    }
    finally
    {
        if (ownsTempDir && Directory.Exists(dbPath))
        {
            try
            {
                Directory.Delete(dbPath, true);
            }
            catch (IOException)
            {
                // Best-effort cleanup of the throwaway temp database.
            }
        }
    }

    return 0;
}
