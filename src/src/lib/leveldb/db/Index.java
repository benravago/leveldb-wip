package lib.leveldb.db;

import java.io.IOException;

import java.util.Iterator;
import java.util.NoSuchElementException;

import lib.util.Iteration;

import lib.leveldb.Slice;
import lib.leveldb.Status;
import lib.leveldb.io.ByteDecoder;
import static lib.leveldb.Status.Code.*;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.TableFile.*;

class Index {

    TableFile table;

    int fileLength;

    int metaindexOffset, metaindexSize;
    int indexOffset, indexSize;

    InternalKey[] dataKey; // high key in data block
    int[] dataOffset, dataSize;

    Filter filter;

    Index(TableFile table) {
        this.table = table;
    }

    Index open() throws IOException {
        fileLength = (int) table.file.length();
        if (fileLength < kFooterEncodedLength) {
            throw new Status("file is too short to be an sstable").state(Corruption);
        }
        // Read the footer block
        readFooter();
        // Read the index block
        readIndex();
        // We've successfully read the footer and the index block:
        // we're ready to serve requests.
        readMeta();

        return this;
    }

    Slice readOptional(int off, int len) {
        try { return table.readContents(off,len); }
        catch (Exception ignore) { return null; }
    }

    void readFooter() {
        var offset = fileLength - kFooterEncodedLength;
        var buf = table.readFully(offset,kFooterEncodedLength);
        var d = new ByteDecoder().wrap(buf);

        d.position(-8);
        var signature = d.getFixed64();
        assert (signature == kTableMagicNumber);
        d.position(0);

        metaindexOffset = d.getVarint32();
        metaindexSize = d.getVarint32();
        indexOffset = d.getVarint32();
        indexSize = d.getVarint32();
    }

    void readIndex() {
        var indexData = table.readContents(indexOffset,indexSize);
        var numRestarts = Block.restartCount(indexData);
        dataKey = new InternalKey[numRestarts];
        dataOffset = new int[numRestarts];
        dataSize = new int[numRestarts];
        var i = 0;
        for (var e : spans(indexData)) {
            dataKey[i] = parseInternalKey(indexData.data,e.keyOffset,e.keyLength);
            dataOffset[i] = e.dataOffset;
            dataSize[i] = e.dataSize;
            i++;
        }
    }

    void readMeta() {
        var meta = readOptional(metaindexOffset,metaindexSize);
        if (meta == null) {
            // Do not propagate errors since meta info is not needed for operation
            return;
        }
        var policy = table.filterPolicy;
        for (var e : spans(meta)) {
            if (startsWith(Filter.key, meta.data,e.keyOffset,e.keyLength ) && policy != null) {
                var filterData = table.readContents(e.dataOffset,e.dataSize);
                filter = Filter.blockReader(policy,filterData);
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

    // used for enumerating the file index and metaindex blocks

    static class Span {
        int keyOffset, keyLength;
        int dataOffset, dataSize;
    }

    static final int sizeof_uint32_t = 4;

    static Iterable<Span> spans(Slice contents) {
        assert (contents.offset == 0);
        var d = new ByteDecoder().wrap(contents);
        d.position(-sizeof_uint32_t);
        var numRestarts = d.getFixed32();
        if (numRestarts == 0) return Iteration.empty;

        return () -> new Iterator<Span>() {
            @Override
            public boolean hasNext() {
                return i < numRestarts;
            }
            @Override
            public Span next() {
                if (hasNext()) return item(i++);
                throw new NoSuchElementException();
            }
            Span item(int i) {
                d.position(-((1+numRestarts-i)*sizeof_uint32_t));
                var offset = d.getFixed32(); // restart[n]
                d.position(offset);
                var z = d.getVarint32(); // sharedBytes
                item.keyLength = d.getVarint32(); // unsharedBytes
                d.getVarint32(); // valueLength
                assert (z == 0);
                item.keyOffset = d.position();
                d.skip(item.keyLength);
                item.dataOffset = d.getVarint32();
                item.dataSize = d.getVarint32();
                return item;
            }
            Span item = new Span();
            int i = 0;
        };
    }

}
