using Silex;
using System.Diagnostics;

if (Directory.Exists("db"))
{
    Directory.Delete("db", true);
}

var options = new StorageOptions { MemTableSizeLimit = 1.MiB(), FlushPeriod = TimeSpan.Zero };
var db = await LsmStorage.OpenAsync<int, int>("db", options);

var data = Enumerable.Range(0, 1_000_000).Select(x => Random.Shared.Next()).ToList(); 

var sw = Stopwatch.StartNew();
data.ForEach(x => db.Put(x, x));
//await db.CloseAsync();
sw.Stop();

Console.WriteLine(sw.Elapsed);

foreach (var file in Directory.GetFiles("db"))
{
    Console.WriteLine($"{file} ({new FileInfo(file).Length})");
}
