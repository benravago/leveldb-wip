package bsd.leveldb.db;

import java.io.Closeable;

import java.util.Iterator;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import bsd.leveldb.Slice;
import bsd.leveldb.io.BinarySearch;
import bsd.leveldb.io.ByteDecoder;
import bsd.leveldb.Cursor;
import bsd.leveldb.io.Iteration;
import bsd.leveldb.io.Varint;
import static bsd.leveldb.db.DbFormat.*;

class Block implements Closeable, BinarySearch.Array<InternalKey> {

    final Slice contents;  // underlying block contents
    final Comparator<InternalKey> cmp;
    final int trailer;     // Offset of restart array (list of fixed32)
    final int[] restart;   // Number of uint32_t entries in restart array

    Block(Slice contents, Comparator<InternalKey> cmp) {
        assert (contents.offset == 0);
        this.contents = contents;
        this.cmp = cmp;
        int[] r = restartList(contents);
        trailer = r[1]; // [count,offset]
        restart = restarts(contents,r[0],trailer);
    }

    @Override
    public void close() { // Block::~Block()
        // if (owned_) {
        //   delete[] data_;
        // }
    }

    static int[] restarts(Slice block, int count, int offset) {
        int[] r = new int[count];
        for (int i = 0, j = offset; i < r.length; i++, j += 4) {
            r[i] = ByteDecoder.decodeFixed32(block.data,j);
        }
        return r;
    }

    static int[] restartList(Slice block) {
        int numRestarts = restartCount(block);
        return new int[]{ numRestarts, (block.offset + block.length) - ((numRestarts + 1) * sizeof_uint32_t) };
    }

    static int restartCount(Slice block) {
        return ByteDecoder.decodeFixed32( block.data, ((block.offset + block.length) - sizeof_uint32_t) );
    }

    static final int sizeof_uint32_t = 4;

    static class Element {
        int sharedBytes;    // key prefix length
        int unsharedBytes;  // key suffix length
        int valueLength;
        int deltaOffset;    // start of key_suffix/value bytes
    }

    Iterator<Element> elements() {
        return elements(contents.offset,trailer);
    }

    Iterator<Element> elements(int i) {
        int start = restart[i];
        int end = (i+1) < restart.length ? restart[i+1] : trailer;
        return elements(start,end-start);
    }

    Iterator<Element> elements(int offset, int length) {
        return elements(new ByteDecoder().wrap(contents.data,offset,length));
    }

    static Iterator<Element> elements(ByteDecoder d) {
        return new Iterator<Element>() {
            @Override
            public boolean hasNext() {
                return d.remaining() > 0;
            }
            @Override
            public Element next() {
                if (!hasNext()) throw new NoSuchElementException();
                element.sharedBytes = d.getVarint32();
                element.unsharedBytes = d.getVarint32();
                element.valueLength = d.getVarint32();
                element.deltaOffset = d.position();
                d.skip(element.unsharedBytes+element.valueLength);
                return element;
            }
            Element element = new Element();
        };
    }

    Cursor<InternalKey,Slice> newIterator() {
        return newIterator(elements());
    }

    Cursor<InternalKey,Slice> newIterator(Iterator<Element> x) {
      return new Cursor<InternalKey,Slice>() {

        @Override
        public boolean hasNext() {
            return elements.hasNext();
        }
        @Override
        public Entry<InternalKey, Slice> next() {
            next = elements.next();
            p = k; k = null; v = null;
            return this;
        }

        // Cursor state fields
        Iterator<Element> elements = x;
        Element next;
        InternalKey k, p;
        Slice v;

        @Override public Slice getValue() { return v != null ? v : (v=value(contents,next)); }
        @Override public InternalKey getKey() { return k != null ? k : (k=key(contents,next,p)); }
      };
    }

    static Slice value(Slice c, Element e) {
        return new Slice( c.data, e.deltaOffset + e.unsharedBytes, e.valueLength );
    }
    static InternalKey key(Slice c, Element e, InternalKey k) {
        if (e.sharedBytes == 0) {
            return parseInternalKey(c.data, e.deltaOffset, e.unsharedBytes );
        } else {
            byte[] b = new byte[ e.sharedBytes + e.unsharedBytes ];
            System.arraycopy(k.userKey.data,k.userKey.offset, b,0, e.sharedBytes );
            System.arraycopy(c.data,e.deltaOffset, b,e.sharedBytes, e.unsharedBytes );
            return parseInternalKey( b,0,b.length );
        }
    }

    static InternalKey delta(Slice c, Element e, InternalKey k) {
        byte[] d;
        int off = e.sharedBytes, len = off + e.unsharedBytes;
        if (off == 0) {
            d = new byte[ 64 > len ? 64 : len];
        } else {
            d = k.userKey.data; // reuse byte[] from previous delta()
            if (len > d.length) {
                d = new byte[ (((len/64)+1)*64) ];  // grow in 64 byte increments
                System.arraycopy( k.userKey.data,k.userKey.offset, d,0 ,e.sharedBytes );
            }
        }
        System.arraycopy( c.data,e.deltaOffset, d,off, e.unsharedBytes );
        return parseInternalKey(d,0,len);
    }

    Entry<InternalKey,Slice> search(InternalKey target) {

        // Binary search in restart array to find the last restart point
        // with a key <= target
        int r = BinarySearch.floor(this,target,cmp);
        if (r < 0) return null;

        // Linear search (within restart block) for key == target
        Iterator<Element> e = elements(r);
        InternalKey d = null;
        while (e.hasNext()) {
            Element next = e.next();
            d = delta(contents,next,d);
            r = cmp.compare(d,target);
            if (r < 0) continue;
            if (r > 0) break;
            return Struct.entry(d, value(contents,next));
        }
        return null;
    }

    // used for BinarySearch on the restart keys
    @Override public int size() { return restart.length; }
    @Override public InternalKey get(int i) { return restartKey(contents.data,restart[i]); }

    static InternalKey restartKey(byte[] b, int offset) {
        long[] s = Varint.load(b,(offset)); // sharedBytes
        long[] u = Varint.load(b,(int)s[1]); // unsharedBytes
        long[] v = Varint.load(b,(int)u[1]); // valueLength; keyBytes is position after valueLength
        return parseInternalKey(b,(int)v[1],(int)u[0]); // (data,keyBytes,unsharedBytes);
    }

    // used for enumerating the file index and metaindex blocks

    static class Index {
        int keyOffset, keyLength;
        int dataOffset, dataSize;
    }

    static Iterable<Index> index(Slice contents) {
        assert (contents.offset == 0);
        ByteDecoder d = new ByteDecoder().wrap(contents);
        d.position(-sizeof_uint32_t);
        int numRestarts = d.getFixed32();
        if (numRestarts == 0) return Iteration.empty;

        return () -> new Iterator<Index>() {
            @Override
            public boolean hasNext() {
                return i < numRestarts;
            }
            @Override
            public Index next() {
                if (!hasNext()) throw new NoSuchElementException();
                d.position(-((1+numRestarts-i)*sizeof_uint32_t)); i++;
                int offset = d.getFixed32(); // restart[n]
                d.position(offset);
                int z = d.getVarint32(); // sharedBytes
                item.keyLength = d.getVarint32(); // unsharedBytes
                d.getVarint32(); // valueLength
                assert (z == 0);
                item.keyOffset = d.position();
                d.skip(item.keyLength);
                item.dataOffset = d.getVarint32();
                item.dataSize = d.getVarint32();
                return item;
            }
            Index item = new Index();
            int i = 0;
        };
    }

}
