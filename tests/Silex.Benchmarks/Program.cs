using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

BenchmarkRunner.Run(typeof(Program).Assembly);

[MemoryDiagnoser, ShortRunJob]
public class Benchmarks
{
}
