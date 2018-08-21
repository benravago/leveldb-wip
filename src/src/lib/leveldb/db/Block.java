package lib.leveldb.db;

import java.io.Closeable;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import lib.util.Varint;
import lib.util.BinarySearch;

import lib.leveldb.Slice;
import lib.leveldb.Cursor;
import lib.leveldb.io.ByteDecoder;
import static lib.leveldb.io.ByteDecoder.*;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.io.Cursors.*;

class Block implements Closeable {

    final Slice contents;  // underlying block contents
    final KeyComparator<InternalKey> cmp;

    final int restarts;    // Number of uint32_t entries in restart array
    final int trailer;     // Offset of restart array (list of fixed32)

    TOC toc;               // restart keys and offsets

    Block(Slice contents, KeyComparator<InternalKey> cmp) {
        assert (contents.offset == 0);
        this.contents = contents;
        this.cmp = cmp;
        restarts = restartCount(contents);
        trailer = restartOffset(contents,restarts);
    }

    @Override
    public void close() { // Block::~Block()
        // if (owned_) {
        //   delete[] data_;
        // }
    }

    static int restartCount(Slice block) {
        return decodeFixed32( block.data, ((block.offset + block.length) - sizeof_uint32_t) );
    }

    static int restartOffset(Slice block, int count) {
        return (block.offset + block.length) - ((count + 1) * sizeof_uint32_t);
    }

    static class Element {
        int sharedBytes;    // key prefix length
        int unsharedBytes;  // key suffix length
        int valueLength;
        int deltaOffset;    // start of key_suffix/value bytes
    }

    Iterator<Element> elements() {
        return elements(contents.offset,trailer);
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
                if (hasNext()) return item();
                throw new NoSuchElementException();
            }
            Element item() {
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

    Iterator<Element> elements(int r) {
        var offset = toc.offset;
        var start = offset[r++];
        var end = r < offset.length ? offset[r] : trailer;
        return elements(start,end-start);
    }

    Cursor<InternalKey,Slice> newIterator() {
        return newIterator(elements());
    }

    Cursor<InternalKey,Slice> newIterator(Iterator<Element> elements) {
        return new Cursor<InternalKey,Slice>() {

            @Override
            public boolean hasNext() {
                return elements.hasNext();
            }
            @Override
            public Entry<InternalKey, Slice> next() {
                next = elements.next();
                d = delta(next,buf,d);
                k = null; v = null;
                return this;
            }

            Element next;
            byte[] buf = contents.data;
            byte[] d = new byte[0];
            InternalKey k;
            Slice v;

            @Override public Slice getValue() { return v != null ? v : (v=value(next,buf)); }
            @Override public InternalKey getKey() { return k != null ? k : (k=key(next,d)); }
        };

    }

    static Slice value(Element e, byte[] buf) {
        return new Slice( buf, e.deltaOffset + e.unsharedBytes, e.valueLength );
    }
    static InternalKey key(Element e, byte[] d) {
        return parseInternalKey( d,0, e.sharedBytes + e.unsharedBytes );
    }
    static byte[] delta(Element e, byte[] buf, byte[] d) {
        var n = e.sharedBytes + e.unsharedBytes;
        if (n > d.length) {
            var s = d; d = new byte[n+16];
            System.arraycopy( s,0, d,0, e.sharedBytes );
        }
        System.arraycopy( buf,e.deltaOffset, d,e.sharedBytes, e.unsharedBytes );
        return d;
    }

    Entry<InternalKey,Slice> seek(InternalKey k) {
        // Binary search in restart array to find the last restart point
        // with a key <= target
        var r = BinarySearch.floor(toc(),k,cmp);
        if (r != 0) return null;
        // Linear search (within restart block) for first key >= target
        return seek(k,r);
    }

    Entry<InternalKey,Slice> seek(InternalKey k, int r) {
        var bc = cmp.comparator();

        var key = k.userKey.data;
        var keyOff = k.userKey.offset;
        var keyPos = keyOff;
        var keyLen = k.userKey.length;

        var delta = contents.data;
        var deltaLen = 0;

        var iter = elements(r);
        Element e = null;

        // find element where k.userKey == delta.key
        while (iter.hasNext()) {
            e = iter.next();

            if (e.unsharedBytes <= sizeof_SequenceAndType) {
                // ignore elements where only the sequence number changed
                continue;
            }

            var p = keyOff + e.sharedBytes;
            if (keyPos < p) {
                // last difference is within shared key bytes;
                // skip this element since key comparison result would be the same
                continue;
            }
            keyPos = p;

            deltaLen = e.unsharedBytes - sizeof_SequenceAndType;
            p = bc.compare( key,keyPos,keyLen, delta,e.deltaOffset,deltaLen );
            if (p < 0) {
                // no match; key will be less than all subsequent keys
                return null;
            }
            if (p > 0) {
                // key is greater than current element; retain diff point for next round
                keyPos = p - 1;
                continue;
            }
            // keys.match; get nearest sequence number
            break;
        }

        var keySeq = sequenceNumber(k);
        var deltaSeq = nextSeq(0, delta, e.deltaOffset+deltaLen, sizeof_SequenceAndType );

        // find element where k.sequence >= delta.sequence
        while (keySeq < decodeSequence(deltaSeq)) {
            if (!iter.hasNext()) {
                return null;
            }
            e = iter.next();
            if (e.unsharedBytes > sizeof_SequenceAndType) {
                // key has changed; skip rest of block
                return null;
            }
            var u = e.unsharedBytes * 8;
            deltaSeq = (deltaSeq << u) >>> u;
            deltaSeq = nextSeq(deltaSeq,delta,e.deltaOffset,e.unsharedBytes);
        }

        var found = new InternalKey( k.userKey, decodeSequenceAndType(deltaSeq) );
        var value = (valueType(found) == kTypeValue)
                  ? new Slice( delta, e.deltaOffset + e.unsharedBytes, e.valueLength )
                  : null;

        return entry(found,value);
    }

    static long nextSeq(long l, byte[] b, int i, int n) {
        switch (n) {
            case 8: l |= ((b[i++] & 0x0ffL)      );
            case 7: l |= ((b[i++] & 0x0ffL) <<  8);
            case 6: l |= ((b[i++] & 0x0ffL) << 16);
            case 5: l |= ((b[i++] & 0x0ffL) << 24);
            case 4: l |= ((b[i++] & 0x0ffL) << 32);
            case 3: l |= ((b[i++] & 0x0ffL) << 40);
            case 2: l |= ((b[i++] & 0x0ffL) << 48);
            case 1: l |= ((b[i  ] & 0x0ffL) << 56);
        }
        return l;
    }

    class TOC implements BinarySearch.Array<InternalKey> {
        InternalKey[] key;
        int[] offset;
        @Override public int size() { return key.length; }
        @Override public InternalKey get(int i) { return key[i]; }
    }

    TOC toc() {
        return toc != null ? toc : (toc = makeToc());
    }

    TOC makeToc() {
        var r = new TOC();
        r.key = new InternalKey[restarts];
        r.offset = new int[restarts];
        var b = contents.data;
        for (int i = 0, x = trailer; i < restarts; i++, x += 4) {
            var offset = decodeFixed32(b,x);
            r.key[i] = restartKey(b,offset);
            r.offset[i] = offset;
        }
        return r;
    }

    static InternalKey restartKey(byte[] b, int offset) {
        var l = new long[1];
        var off = Varint.load(l,b,offset); // sharedBytes; should be 0
        assert (l[0] == 0);
        off = Varint.load(l,b,off); // unsharedBytes; would be keyLength
        var len = (int) l[0];
        off = Varint.load(l,b,off); // valueLength; keyOffset is position after valueLength
        return parseInternalKey(b,off,len); // (data,keyOffset,keyLength);
    }

}
