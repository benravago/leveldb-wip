package lib.leveldb.io;

import lib.leveldb.Slice;
import lib.util.Varint;

/**
 * Endian-neutral decoding:
 * <ul>
 * <li> Fixed-length numbers are encoded with least-significant byte first
 * <li> In addition we support variable length "varint" encoding
 * <li> Strings are encoded prefixed by their length in varint format
 * </ul>
 */
public class ByteDecoder {

    byte[] buf;
    int off;
    int limit;

    public ByteDecoder wrap(byte[] buf, int off, int len) {
        this.buf = buf;
        this.off = off;
        this.limit = off + len;
        return this;
    }

    public ByteDecoder wrap(byte[] b) {
        return wrap(b,0,b.length);
    }

    public ByteDecoder wrap(Slice s) {
        return wrap(s.data,s.offset,s.length);
    }

    public void skip(int n) {
        off += n;
        if (off > limit) off = limit;
    }

    public void position(int off) {
        if (off < 0) off = limit + off;
        assert (0 <= off && off <= limit);
        this.off = off;
    }

    // 0 <= mark <= position <= limit <= capacity

    public int position() { return off; }
    public int limit() { return limit; }
    public int remaining() { return limit - off; }

    public static int decodeFixed8(byte[] b, int o) {
        return (b[o] & 0x0ff);
    }

    public static int decodeFixed16(byte[] b, int o) {
        return (b[o++] & 0x0ff)
             | (b[o  ] & 0x0ff) << 8 ;
    }

    public static int decodeFixed32(byte[] b, int o) {
        return (b[o++] & 0x0ff)
             | (b[o++] & 0x0ff) <<  8
             | (b[o++] & 0x0ff) << 16
             | (b[o  ] & 0x0ff) << 24 ;
    }

    public static long decodeFixed64(byte[] b, int o) {
        return (b[o++] & 0x0ffL)
             | (b[o++] & 0x0ffL) <<  8
             | (b[o++] & 0x0ffL) << 16
             | (b[o++] & 0x0ffL) << 24
             | (b[o++] & 0x0ffL) << 32
             | (b[o++] & 0x0ffL) << 40
             | (b[o++] & 0x0ffL) << 48
             | (b[o  ] & 0x0ffL) << 56 ;
    }

    // Standard Get... routines parse a value from the beginning of a Slice
    // and advance the slice past the parsed value.

    public int getFixed8() {
        return buf[off++] & 0x0ff;
    }

    public int getFixed32() {
        int o = off;
        off += 4;
        assert (off <= limit);
        return decodeFixed32(buf,o);
    }

    public long getFixed64() {
        int o = off;
        off += 8;
        assert (off <= limit);
        return decodeFixed64(buf,o);
    }

    public int getVarint32() {
        long[] l = new long[1];
        off = Varint.load(l,buf,off);
        assert (off <= limit);
        return (int) l[0];
    }

    public long getVarint64() {
        long[] l = new long[1];
        off = Varint.load(l,buf,off);
        assert (off <= limit);
        return l[0];
    }

    int[] getLengthPrefix() {
        long[] l = new long[1];
        int offset = Varint.load(l,buf,off);
        int length = (int) l[0];
        off = offset + length;
        assert (off <= limit);
        return new int[]{offset,length};
    }

    public String getLengthPrefixedString() {
        int[] p = getLengthPrefix();
        return new String(buf,p[0],p[1]);
    }

    public Slice getLengthPrefixedSlice() {
        int[] p = getLengthPrefix();
        return new Slice(buf,p[0],p[1]);
    }

    public byte getByte() {
        return buf[off++];
    }

    public byte[] getBytes(int size) {
        byte[] dst = new byte[size];
        System.arraycopy(buf,off,dst,0,size);
        off += size;
        return dst;
    }

}
