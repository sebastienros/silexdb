using Silex;
using System.Buffers.Binary;
using System.Diagnostics;

await Store1MillionValues();

static async Task Store1MillionValues()
{
    if (Directory.Exists("db"))
    {
        Directory.Delete("db", true);
    }

    var options = new StorageOptions { MemTableSizeLimit = 1.MiB(), FlushPeriod = TimeSpan.Zero };
    var db = await LsmStorage.OpenAsync<uint>("db", options);

    var data = Enumerable.Range(0, 1_000_000).Select(x => Random.Shared.Next()).ToList();

    var sw = Stopwatch.StartNew();
    data.ForEach(x => db.Put((uint)x, Encode(x)));
    Console.WriteLine($"Add entries: {sw.Elapsed}");

    sw.Restart();
    await db.CloseAsync();
    sw.Stop();

    Console.WriteLine($"Save to disk: {sw.Elapsed}");

    foreach (var file in Directory.GetFiles("db"))
    {
        Console.WriteLine($"{file} ({new FileInfo(file).Length})");
    }

    static byte[] Encode(int value)
    {
        var buffer = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        return buffer;
    }
}

