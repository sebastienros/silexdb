using Silex;
using System.Diagnostics;

await Store1MillionValues();

static async Task Store1MillionValues()
{
    if (Directory.Exists("db"))
    {
        Directory.Delete("db", true);
    }

    var options = new StorageOptions { MemTableSizeLimit = 1.MiB(), FlushPeriod = TimeSpan.Zero };
    var db = await LsmStorage.OpenAsync("db", options);

    var data = Enumerable.Range(0, 1_000_000).Select(x => Random.Shared.Next()).ToList();

    var sw = Stopwatch.StartNew();
    data.ForEach(x => db.Put(x, x));
    Console.WriteLine($"Add entries: {sw.Elapsed}");

    sw.Restart();
    await db.CloseAsync();
    sw.Stop();

    Console.WriteLine($"Save to disk: {sw.Elapsed}");

    foreach (var file in Directory.GetFiles("db"))
    {
        Console.WriteLine($"{file} ({new FileInfo(file).Length})");
    }
}
