using System.Text.Json.Serialization;

namespace Silex;

[JsonSerializable(typeof(Manifest))]
[JsonSerializable(typeof(StorageBackup))]
internal partial class StorageJsonContext : JsonSerializerContext;
