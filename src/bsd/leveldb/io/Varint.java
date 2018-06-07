package bsd.leveldb.io;

public interface Varint {

    static final int MAX_BYTES = 9;
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
        long l = value & 0x07fffffffffffffffL; // only use 63 bits
        while (l > 0x07fL) {
            b[off++] = (byte)( (l & 0x07fL) | 0x080L );
            l >>>= 7;
        }
        b[off++] = (byte)( l );
        return off;
    }

    static int width(long l) {
        if (l < 0) l &= 0x07fffffffffffffffL;
        if (l < 0x080L) return 1; // 7 bits
        if (l < 0x04000L) return 2; // 14 bits
        if (l < 0x0200000L) return 3; // 21 bits
        if (l < 0x010000000L) return 4; // 28 bits
        if (l < 0x0800000000L) return 5; // 35 bits
        if (l < 0x040000000000L) return 6; // 42 bits
        if (l < 0x02000000000000L) return 7; // 49 bits
        if (l < 0x0100000000000000L) return 8; // 56 bits
        /* (l < 0x08000000000000000L*/ return 9; // 63 bits
    }

}
