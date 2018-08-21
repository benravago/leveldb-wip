package lib.leveldb.db;

import java.io.IOException;

import java.util.Iterator;
import java.util.Map;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import lib.io.SeekableInputStream;
import lib.util.BinarySearch;
import lib.util.concurrent.MutexLock;

import lib.leveldb.Cursor;
import lib.leveldb.Slice;
import lib.leveldb.Status;
import lib.leveldb.DB.FilterPolicy;
import lib.leveldb.io.SnappyDecoder;
import static lib.leveldb.Status.Code.*;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.io.ByteDecoder.*;

class TableFile implements Table {

    // Maximum encoding length of a BlockHandle
    static final int kBlockHandleMaxEncodedLength = 10 + 10;

    // Encoded length of a Footer.  Note that the serialization of a
    // Footer will always occupy exactly this many bytes.  It consists
    // of two block handles and a magic number.
    static final int kFooterEncodedLength = 2 * kBlockHandleMaxEncodedLength + 8;

    // kTableMagicNumber was picked by running
    //    echo http://code.google.com/p/leveldb/ | sha1sum
    // and taking the leading 64 bits.
    static final long kTableMagicNumber = 0x0db4775248b80fb57L;

    // 1-byte type + 32-bit crc
    static final int kBlockTrailerSize = 5;

    // DB contents are stored in a set of blocks, each of which holds a
    // sequence of key,value pairs.  Each block may be compressed before
    // being stored in a file.  The following enum describes which
    // compression method (if any) is used to compress a block.
    /* enum CompressionType */
    static final int kNoCompression = 0x00;
    static final int kSnappyCompression = 0x01;


    final MutexLock read;
    final SeekableInputStream file;
    final KeyComparator<InternalKey> icmp;

    Checksum checksum = new CRC32C();
    FilterPolicy filterPolicy;

    BinarySearch.Array<InternalKey> dataKey; // high key in data block
    int[] dataOffset, dataSize;

    Filter filter;

    Map<Long,Block> cache;
    int fileId;

    /**
     *  key is {fileNumber,offset} pair as a long
     */
    static long cacheKey(long hi, long lo) {
        return (hi << 32) | (lo & 0x0ffffffff);
    }

    TableFile(SeekableInputStream file, KeyComparator<InternalKey> icmp) {
        this.file = file;
        this.icmp = icmp;
        this.read = new MutexLock();
    }
    TableFile filterPolicy(FilterPolicy policy) {
        filterPolicy = policy; return this;
    }
    TableFile verifyChecksums(boolean check) {
        checksum = check ? new CRC32C() : null; return this;
    }
    TableFile cache(Map<Long,Block> blockCache, int cacheId) {
        cache = blockCache; fileId = cacheId; return this;
    }

    @Override
    public void close() { // Table::~Table() {
        // delete rep_;
    }

    //  ~Rep() {
    //    delete filter;
    //    delete [] filter_data;
    //    delete index_block;
    //  }

    /**
     * Attempt to open the table that is stored in bytes [0..file_size) of "file",
     * and read the metadata entries necessary to allow retrieving data from the table.
     *
     * If successful, returns ok and sets "*table" to the newly opened table.
     * The client should delete "*table" when no longer needed.
     * If there was an error while initializing the table,
     * sets "*table" to NULL and returns a non-ok status.
     * Does not take ownership of "*source",
     * but the client must ensure that "source" remains live
     * for the duration of the returned table's lifetime.
     *
     * *file must remain live while this Table is in use.
     */
    TableFile open() { // Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table)
        try {
            var index = new Index(this).open();
            dataOffset = index.dataOffset;
            dataSize = index.dataSize;
            dataKey = BinarySearch.array(index.dataKey);
            filter = index.filter;
            return this;
        }
        catch (IOException e) { throw new Status(e).state(IOError); }
    }

    byte[] readFully(long off, int len) {
        try {
            var b = new byte[len];
            file.seek(off);
            if ((file.read(b) != len)) {
                throw new Status("could not read "+len+" bytes from offset "+off).state(Corruption);
            }
            return b;
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }
    }

    /**
     * Read the block identified by "handle" from "file".
     * On failure return non-OK.  On success fill *result and return OK.
     */
    Slice readContents(int offset, int size) {

        // Read the block contents as well as the type/crc footer.
        // See table_builder.cc for the code that built this structure.

        var buf = readFully(offset, size + kBlockTrailerSize );

        // Check the crc of the type and the block contents
        if (checksum != null) {
            var n = size + 1;
            var crc = LogFormat.unmask(decodeFixed32(buf,n));
            checksum.reset();
            checksum.update(buf,0,n);
            var actual = (int) checksum.getValue();
            if (actual != crc) {
                throw new Status("block checksum mismatch").state(Corruption);
            }
        }

        switch(decodeFixed8(buf,size)) {
            case kNoCompression: return new Slice(buf,0,size);
            case kSnappyCompression: return SnappyDecoder.decode(buf,0,size);
            default: throw new Status("bad block type").state(Corruption);
        }
    }

    // Iterator* Table::NewIterator(const ReadOptions& options) const {
    //   return NewTwoLevelIterator(
    //     rep_->index_block->NewIterator(rep_->options.comparator),
    //     &Table::BlockReader, const_cast<Table*>(this), options);

    @Override
    public Cursor<InternalKey,Slice> newIterator(boolean fillCache) {
        return new TwoLevelIterator<>(
            indexIterator(dataOffset.length),
            (index) -> blockReader(dataOffset[index],dataSize[index],fillCache).newIterator()
        );
    }

    Iterator<Integer> indexIterator(int limit) {
        return new Iterator<Integer>() {
            int index = 0;
            @Override public boolean hasNext() { return index < limit; }
            @Override public Integer next() { return index++; }
        };
    }

    Block blockReader(int offset, int length, boolean fillCache) {
        var fileNumberOffset = cacheKey(fileId,offset);
        if (cache != null) {
            var block = cache.get(fileNumberOffset);
            if (block != null) {
                return block;
            }
        }
        var data = readContents(offset,length);
        var block = new Block(data,icmp);
        if (cache != null && fillCache) {
            cache.put(fileNumberOffset,block);
        }
        return block;
    }

    @Override
    public Map.Entry<InternalKey,Slice> internalGet(InternalKey k, boolean fillCache) {

        // Binary search in file restart array to find the
        // first restart point with a key >= target
        var s = BinarySearch.ceiling(dataKey,k,icmp); // iiter->Seek(k);
        if (s < 0) {
            return null; // Not found
        }
        var offset = dataOffset[s];
        if (filter != null) {
            // consult the filter if a match can be expected
            if (!filter.keyMayMatch(offset,k.userKey)) {
                return null; // Not found
            }
        }
        // Search for the key in the table file block
        return blockReader(offset,dataSize[s],fillCache).seek(k);
    }

}
