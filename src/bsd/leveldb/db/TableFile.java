package bsd.leveldb.db;

import java.io.IOException;

import java.util.Iterator;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import bsd.leveldb.Slice;
import bsd.leveldb.Cursor;
import bsd.leveldb.Status;
import bsd.leveldb.io.Cache;
import bsd.leveldb.io.MutexLock;
import bsd.leveldb.io.ByteDecoder;
import bsd.leveldb.io.BinarySearch;
import bsd.leveldb.io.SnappyDecoder;
import bsd.leveldb.io.SeekableInputStream;
import static bsd.leveldb.io.ByteDecoder.*;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.Status.Code.*;

class TableFile implements Table, BinarySearch.Array<InternalKey> {

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


    SeekableInputStream file;
    MutexLock read;

    int fileId;
    Map<Long,Block> cache; // key is {fileNumber,offset} pair as a long

    Comparator<InternalKey> icmp;
    FilterPolicy filterPolicy;
    Checksum checksum = new CRC32C();

    int metaindexOffset;
    int metaindexSize;
    int indexOffset;
    int indexSize;

    Slice indexData;
    InternalKey[] dataKey; // high key in data block
    int[] dataOffset, dataSize;

    Slice filterData;
    Filter filter;

    TableFile(SeekableInputStream file, Comparator<InternalKey> icmp) {
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
    TableFile cache(Cache<Long,Block> blockCache, int cacheId) {
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

    @Override
    public Cursor<InternalKey,Slice> newIterator(boolean fillCache) { // Iterator* Table::NewIterator(const ReadOptions& options) const {
        return new TwoLevelIterator<>(
            indexIterator(dataOffset.length),
            (index) -> blockReader(dataOffset[index],dataSize[index],fillCache).newIterator()
        );
        // return NewTwoLevelIterator(
        //     rep_->index_block->NewIterator(rep_->options.comparator),
        //     &Table::BlockReader, const_cast<Table*>(this), options);
    }

    Iterator<Integer> indexIterator(int limit) {
        return new Iterator<Integer>() {
            int index = 0;
            @Override public boolean hasNext() { return index < limit; }
            @Override public Integer next() { return index++; }
        };
    }

    Block blockReader(int offset, int length, boolean fillCache) {
        long fileNumberOffset = Struct.join(fileId,offset);
        if (cache != null) {
            Block bucket = cache.get(fileNumberOffset);
            if (bucket != null) {
                return bucket;
            }
        }
        Slice data = readContents(offset,length);
        Block bucket = new Block(data,icmp);
        if (cache != null && fillCache) {
            cache.put(fileNumberOffset,bucket);
        }
        return bucket;
    }

    @Override
    public Entry<InternalKey,Slice> internalGet(InternalKey k, boolean fillCache) {

        // Binary search in file restart array to find the
        // first restart point with a key >= target
        int s = BinarySearch.ceiling(this,k,icmp); // iiter->Seek(k);
        if (s < 0) {
            return null; // Not found
        }
        int sectionOffset = dataOffset[s];
        if (filter != null) {
            // consult the filter if a match can be expected
            if (!filter.keyMayMatch(sectionOffset,k.userKey)) {
                return null; // Not found
            }
        }
        // Search for the key in the table file block
        Entry<InternalKey,Slice> blockItem =
            blockReader(sectionOffset,dataSize[s],fillCache)
                .search(k);

        if (blockItem == null) {
            return null; // Not found
        }
        // found it, repackage the key/value from the bucket Cursor
        return Struct.entry(k,blockItem.getValue());
    }

    // used for BinarySearch on the block data keys
    @Override public int size() { return dataKey.length; }
    @Override public InternalKey get(int i) { return dataKey[i]; }

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
        int fileLength;
        try {
            fileLength = (int) file.length();
            if (fileLength < kFooterEncodedLength) {
                throw new Status("file is too short to be an sstable").state(Corruption);
            }
        }
        catch (IOException e) { throw new Status(e).state(IOError); }

        // Read the footer block
        readFooter(fileLength);

        // Read the index block
        indexData = readContents(indexOffset,indexSize);

        // We've successfully read the footer and the index block:
        // we're ready to serve requests.
        makeIndex(); // Block* index_block = new Block(index_block_contents);

        // Read the metaindex block
        if (metaindexSize > 0) {
            readMeta();
        }

        return this;
    }

    byte[] readFully(SeekableInputStream in, long off, int len) {
        try {
            byte[] b = new byte[len];
            in.seek(off);
            if ((in.read(b) != len)) {
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

        byte[] buf = readFully(file, offset, size + kBlockTrailerSize );

        // Check the crc of the type and the block contents
        if (checksum != null) {
            int n = size + 1;
            int crc = LogFormat.unmask(decodeFixed32(buf,n));
            checksum.reset();
            checksum.update(buf,0,n);
            int actual = (int) checksum.getValue();
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

    void readFooter(int fileLength) {
        int offset = fileLength - kFooterEncodedLength;
        byte[] buf = readFully(file,offset,kFooterEncodedLength);
        ByteDecoder d = new ByteDecoder().wrap(buf);

        d.position(-8);
        long signature = d.getFixed64();
        assert (signature == kTableMagicNumber);
        d.position(0);

        metaindexOffset = d.getVarint32();
        metaindexSize = d.getVarint32();
        indexOffset = d.getVarint32();
        indexSize = d.getVarint32();
    }

    void readMeta() {
        Slice contents;
        try {
            contents = readContents(metaindexOffset,metaindexSize);
        }
        catch (Exception ignore) {
            // Do not propagate errors since meta info is not needed for operation
            return;
        }
        for (Block.Index e : Block.index(contents)) {
            if (startsWith(Filter.key, contents.data,e.keyOffset,e.keyLength ) && filterPolicy != null) {
                filterData = readContents(e.dataOffset,e.dataSize);
                filter = Filter.blockReader(filterPolicy,filterData);
            }
            // ignore other blocks; unsupported
        }
    }

    static boolean startsWith(byte[] prefix, byte[] b, int off, int len) {
        if (prefix.length > len) return false;
        for (int i = 0, j = off; i < prefix.length; i++, j++) {
            if (prefix[i] != b[j]) return false;
        }
        return true;
    }

    void makeIndex() {
        int numRestarts = Block.restartCount(indexData);
        dataKey = new InternalKey[numRestarts];
        dataOffset = new int[numRestarts];
        dataSize = new int[numRestarts];
        int i = 0;
        for (Block.Index e : Block.index(indexData)) {
            dataKey[i] = parseInternalKey(indexData.data,e.keyOffset,e.keyLength);
            dataOffset[i] = e.dataOffset;
            dataSize[i] = e.dataSize;
            i++;
        }
    }

}
