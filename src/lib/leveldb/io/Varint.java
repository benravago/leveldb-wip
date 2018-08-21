package bsd.leveldb.io;

public interface Varint {

    static final int MAX_BYTES = 9;  // only use 63 bits
    static final long MAX_BITS = 0x07fffffffffffffffL;

    static byte[] toBytes(long l) {
        byte[] b = new byte[width(l)];
        store(l,b,0);
        return b;
    }

    static long toLong(byte[] b) {
        return load(b,0)[0];
    }

    // returns parsed long value and new offset
    static long[] load(byte[] b, int off) {
        long l = 0;
        int s = 0;
        byte t;
        do {
            t = b[off++];
            l |= (t & 0x07fL) << s;
            s += 7;
        } while (t < 0);
        assert (s < 64);
        return new long[]{ l, off };
    }

    // returns new offset
    static int store(long value, byte[] b, int off) {
        long l = value < 0 ? ((value << 1) >>> 1) : value;
        while (l > 0x07fL) {
            b[off++] = (byte)( (l & 0x07fL) | 0x080L );
            l >>>= 7;
        }
        b[off++] = (byte)( l );
        return off;
    }

    static int width(long l) {
        if (l < 0) l = ((l << 1) >>> 1);
        return 9 - ((Long.numberOfLeadingZeros(l)-1)/7);
    }

}
