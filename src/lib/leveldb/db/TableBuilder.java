package lib.leveldb.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import lib.util.Varint;

import lib.leveldb.Slice;
import lib.leveldb.Status;
import lib.leveldb.DB.FilterPolicy;
import lib.leveldb.io.SnappyEncoder;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.io.ByteEncoder.*;
import static lib.leveldb.db.TableFile.*;

/**
 * TableBuilder provides the interface used to build a Table (an immutable and sorted map from keys to values).
 *
 * Multiple threads can invoke const methods on a TableBuilder without external synchronization,
 * but if any of the threads may call a non-const method,
 * all threads accessing the same TableBuilder must use external synchronization.
 */
class TableBuilder implements Closeable {

    //   Options options;
    KeyComparator<InternalKey> comparator;
    int blockSize;
    int restartInterval;
    int compressionType;
    //   Options index_block_options;
    final OutputStream file;
    int offset;

    BlockBuilder dataBlock;
    BlockBuilder indexBlock;
    InternalKey lastKey;
    long numEntries = 0;
    boolean closed = false; // Either Finish() or Abandon() has been called.
    Filter.BlockBuilder filterBlock;

    // We do not emit the index entry for a block until
    // we have seen the first key for the next data block.
    // This allows us to use shorter keys in the index block.
    //
    // For example, consider a block boundary between
    // the keys "the quick brown fox" and "the who".
    // We can use "the r" as the key for the index block entry
    // since it is >= all entries in the first block
    // and < all entries in subsequent blocks.
    //
    // Invariant: r->pending_index_entry is true only if data_block is empty.
    boolean pendingIndexEntry;
    long[] pendingHandle; // Handle to add to index block

    static final int OFFSET = 0;
    static final int SIZE = 1;

    static Slice blockHandle(long[] a) {
        var b = new byte[kBlockHandleMaxEncodedLength];
        var c = Varint.store(a[OFFSET],b,0); // offset
        c = Varint.store(a[SIZE],b,c); // size
        return new Slice(b,0,c);
    }

    /**
     * Create a builder that will store the contents of the table it is building in *file.
     * Does not close the file. It is up to the caller to close the file after calling Finish().
     */
    TableBuilder(OutputStream file, KeyComparator<InternalKey> comparator) {
        this.file = file;
        this.comparator = comparator;
        offset = 0;
        closed = false;
        pendingIndexEntry = false;
    }

    @Override
    public void close() { // TableBuilder::~TableBuilder()
        // assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
        // delete rep_->filter_block;
        // delete rep_;
    }

    // provide defaults for blockSize & blockRestartinterval
    TableBuilder block(int size, int interval) {
        blockSize = size;
        restartInterval = interval;
        dataBlock = new BlockBuilder(restartInterval,comparator);
        indexBlock = new BlockBuilder(1,comparator); // index_block_options.block_restart_interval = 1;
        return this;
    }
    TableBuilder filterPolicy(FilterPolicy policy) {
        if (policy != null) {
            filterBlock = Filter.blockBuilder(policy);
            filterBlock.startBlock(0);
        } else {
            filterBlock = null;
        }
        return this;
    }
    TableBuilder compression(int type) {
        compressionType = type;
        return this;
    }

    /**
     * Add key,value to the table being constructed.
     * // REQUIRES: key is after any previously added key according to comparator.
     * // REQUIRES: Finish(), Abandon() have not been called
     */
    void add(InternalKey key, Slice value) { // Add(const Slice& key, const Slice& value);
        assert (!closed);
        if (numEntries > 0) {
            assert(comparator.compare(key,lastKey) > 0);
        }

        if (pendingIndexEntry) {
            assert (dataBlock.isEmpty());
            InternalKey last_key = comparator.findShortestSeparator(lastKey,key);
            indexBlock.add(last_key,blockHandle(pendingHandle));
            pendingIndexEntry = false;
        }

        if (filterBlock != null) {
            filterBlock.addKey(key.userKey);
        }

        lastKey = key;
        numEntries++;
        dataBlock.add(key,value);

        var estimatedBlockSize = dataBlock.currentSizeEstimate();
        if (estimatedBlockSize >= blockSize) {
            try { flush(); }
            catch (IOException e) { throw new Status(e).state(Status.Code.IOError); }
        }
    }

    // Number of calls to Add() so far.
    long numEntries() {
        return numEntries;
    }

    /**
     * Advanced operation: flush any buffered key/value pairs to file.
     * Can be used to ensure that two adjacent entries never live in the same data block.
     * Most clients should not need to use this method.
     * // REQUIRES: Finish(), Abandon() have not been called
     */
    void flush() throws IOException {
        assert (!closed);
        if (dataBlock.isEmpty()) return;
        assert (!pendingIndexEntry);
        pendingHandle = writeBlock(dataBlock);
        pendingIndexEntry = true;
        file.flush();
        if (filterBlock != null) {
            filterBlock.startBlock(offset);
        }
    }

    /**
     * Finish building the table.
     * Stops using the file passed to the constructor after this function returns.
     * // REQUIRES: Finish(), Abandon() have not been called
     */
    void finish() throws IOException {
        flush();
        assert (!closed);
        closed = true;

        long[] filterBlockHandle, metaindexBlockHandle, indexBlockHandle;

        // Write filter block
        if (filterBlock != null) {
            filterBlockHandle = writeRawBlock(filterBlock.finish(),kNoCompression);
        } else {
            filterBlockHandle = null;
        }

        // Write metaindex block
        var metaindexBlock = new BlockBuilder(restartInterval,comparator);
        if (filterBlock != null) {
            // Add mapping from "filter.Name" to location of filter data
            metaindexBlock.add(
                new InternalKey(filterBlock.name(),-1),
                blockHandle(filterBlockHandle));
        }
        metaindexBlockHandle = writeBlock(metaindexBlock);

        // Write index block
        if (pendingIndexEntry) {
            InternalKey last_key = comparator.findShortSuccessor(lastKey);
            indexBlock.add(last_key,blockHandle(pendingHandle));
            pendingIndexEntry = false;
        }
        indexBlockHandle = writeBlock(indexBlock);

        // Write footer
        var footer = new byte[kFooterEncodedLength];
        int p;
        p = Varint.store(metaindexBlockHandle[OFFSET],footer,0);
        p = Varint.store(metaindexBlockHandle[SIZE],footer,p);
        p = Varint.store(indexBlockHandle[OFFSET],footer,p);
            Varint.store(indexBlockHandle[SIZE],footer,p);
        encodeFixed64(kTableMagicNumber,footer,footer.length-8);
        file.write(footer);
        offset += footer.length;
    }

    /**
     * Indicate that the contents of this builder should be abandoned.
     * Stops using the file passed to the constructor after this function returns.
     * If the caller is not going to call Finish(),
     * it must call Abandon() before destroying this builder.
     * // REQUIRES: Finish(), Abandon() have not been called
     */
    void abandon() { // void TableBuilder::Abandon()
        // Rep* r = rep_;
        assert (!closed);
        closed = true;
    }

    /**
     * Size of the file generated so far.
     * If invoked after a successful Finish() call,
     * returns the size of the final generated file.
     */
    long fileSize() { // uint64_t FileSize() const;
        return offset;
    }

    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32

    long[] writeBlock(BlockBuilder block) throws IOException {
        var raw = block.finish();

        Slice blockContents;
        int type = compressionType;
        switch (type) {
            case kNoCompression: {
                blockContents = raw;
                break;
            }
            case kSnappyCompression: {
                var compressed = SnappyEncoder.encode(raw);
                if (compressed.length < raw.length - (raw.length / 8)) {
                    blockContents = compressed;
                } else {
                    // Snappy not supported, or compressed less than 12.5%,
                    // so just store uncompressed form
                    blockContents = raw;
                    type = kNoCompression;
                }
                break;
            }
            default: throw new Status("bad block type").state(Status.Code.InvalidArgument);
        }
        var handle = writeRawBlock(blockContents,type);
        block.reset();
        return handle;
    }

    final Checksum checksum = new CRC32C();

    long[] writeRawBlock(Slice blockContents, int type) throws IOException {
        // write the data section
        var handle = new long[] { offset, blockContents.length };
        file.write( blockContents.data, blockContents.offset, blockContents.length );
        // write the trailer section
        var trailer = new byte[kBlockTrailerSize];
        trailer[0] = (byte) type;
        checksum.reset();
        checksum.update(blockContents.data,blockContents.offset,blockContents.length);
        checksum.update(type);
        var crc = LogFormat.mask((int)checksum.getValue());
        encodeFixed32(crc,trailer,1);
        file.write(trailer);
        // done
        file.flush();
        offset += blockContents.length + kBlockTrailerSize;
        return handle;
    }

}
