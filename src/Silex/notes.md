- The operations in MemTable can be synchronous as only background operations like disk-flushing, or explicit calls to read the filesystem are async.
- Check ByteArrayComparer performance.
- LsmStorageInner may be optimized with a better strategy.
- Store the encoder version/type in the block such that we can switch encoders dynamically. When compaction
  is happening then the old encodings will be replaced. A cli could also migrate the storage to other compaction
- strategies
- Create a custom type for KVP<ROM<byte>, ROM<byte>> since it's used in many places.
- Should encoding be responsible for creating files? We should be able to decide which files are created for an SST (like one for content and one for metadata)
  or where it should be stored (blobs)
- Use IStorageIterator for MemTables and implement a MergeIterator implementing it too. Use it in LsmStorageInner.

- When a value is added to a block (through SstBuilder), the key is encoded in the bloom filter, and encoded again 
in the block memory. It would be better to encode it once in the block memory as they are added.
Then the block could be flushed to disk when it's finished (FinishBlock) such that files are actually written in block size
and the SST is also not in memory as a whole. Then test the performance of with and without `WriteThrough` on the file stream.


## Different memory management buffers:

### RecyclableMemoryStream : IBufferWriter

Grows its buffer as more bytes are written. It does so by creating a list of blocks. The allocated buffers
are returned to the pool when the object is disposed. `GetReadOnlySequence()` returns a `ReadOnlySequence<byte>` 
containing these blocks. `GetBuffer()` returns a single pooled `byte[]` that can be manipulated. 

### PooledArrayBufferWriter

When the internal buffer is full it rents a bigger buffer and copies the previous value to it.