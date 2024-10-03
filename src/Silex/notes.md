- The operations in MemTable can be synchronous as only background operations like disk-flushing, or explicit calls to read the filesystem are async.
- Check ByteArrayComparer performance.
- LsmStorageInner may be optimized with a better strategy.
- Store the encoder version/type in the block such that we can switch encoders dynamically. When compaction
  is happening then the old encodings will be replaced. A cli could also migrate the storage to other compaction
- strategies
- Create a custom type for KVP<ROM<byte>, ROM<byte>> since it's used in many places.
