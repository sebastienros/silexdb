using Silex;
using System.Diagnostics;

if (Directory.Exists("db"))
{
    Directory.Delete("db", true);
}

var options = new StorageOptions { MemTableSizeLimit = 100.KiB() };
var db = await LsmStorage.OpenAsync("db", options);

var data = Enumerable.Range(0, 100_000).Select(x => new Bytes(x)).ToList(); 

var sw = Stopwatch.StartNew();
data.ForEach(x => db.Put(x, x));
await db.CloseAsync();
sw.Stop();

Console.WriteLine(sw.Elapsed);
foreach (var file in Directory.GetFiles("db"))
{
    Console.WriteLine($"{file} ({new FileInfo(file).Length})");
}
