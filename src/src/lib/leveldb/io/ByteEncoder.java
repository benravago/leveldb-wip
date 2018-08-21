package lib.leveldb.io;

import java.io.ByteArrayOutputStream;

import lib.leveldb.Slice;
import lib.util.Varint;

/**
 * Endian-neutral encoding:
 * <ul>
 * <li> Fixed-length numbers are encoded with least-significant byte first
 * <li> In addition we support variable length "varint" encoding
 * <li> Strings are encoded prefixed by their length in varint format
 * </ul>
 */
public class ByteEncoder extends ByteArrayOutputStream {

    byte[] var = new byte[Varint.MAX_BYTES+1];
    byte[] f32 = new byte[4];
    byte[] f64 = new byte[8];

    public static void encodeFixed8(int v, byte[] b, int o) {
        b[o  ] = (byte)(v);
    }

    public static void encodeFixed16(int v, byte[] b, int o) {
        b[o++] = (byte)(v);
        b[o  ] = (byte)(v >> 8);
    }

    public static void encodeFixed32(int v, byte[] b, int o) {
        b[o++] = (byte)(v);
        b[o++] = (byte)(v >> 8);
        b[o++] = (byte)(v >> 16);
        b[o  ] = (byte)(v >> 24);
    }

    public static void encodeFixed64(long v, byte[] b, int o) {
        b[o++] = (byte)(v);
        b[o++] = (byte)(v >> 8);
        b[o++] = (byte)(v >> 16);
        b[o++] = (byte)(v >> 24);
        b[o++] = (byte)(v >> 32);
        b[o++] = (byte)(v >> 40);
        b[o++] = (byte)(v >> 48);
        b[o  ] = (byte)(v >> 56);
    }

    // Standard Put... routines append to a string

    public void putFixed8(int value) {
        write(value);
    }

    public void putFixed32(int value) {
        encodeFixed32(value,f32,0);
        write(f32,0,4);
    }

    public void putFixed64(long value) {
        encodeFixed64(value,f64,0);
        write(f64,0,8);
    }

    public void putVarint32(int value) {
        putVarint64( value & 0x000000000ffffffffL );
    }

    public void putVarint64(long value) {
        int n = Varint.store(value,var,0);
        write(var,0,n);
    }

    public void putLengthPrefixedString(String s) {
        byte[] b = s.getBytes();
        putVarint64(b.length);
        write(b,0,b.length);
    }

    public void putLengthPrefixedSlice(Slice s) {
        putVarint64(s.length);
        write(s.data,s.offset,s.length);
    }

    public void putSlice(Slice s) {
        write(s.data,s.offset,s.length);
    }

    public void clear() {
        this.count = 0;
    }
    
    public boolean isEmpty() {
        return size() < 1;
    }

    public Slice asSlice() {
        return new Slice(buf,0,count);
    }
    //  or new Slice(toByteArray());
}
