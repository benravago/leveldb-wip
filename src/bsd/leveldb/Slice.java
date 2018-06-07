package bsd.leveldb;

/**
 * Slice is a simple structure containing a pointer into some external storage and a size.
 */
public class Slice {

    public final byte[] data;
    public final int offset;
    public final int length;

    public Slice(byte[] d, int o, int l) {
        data=d; offset=o; length=l;
    }

    public Slice(byte[] b) {
        this(b,0,b.length);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o instanceof Slice) {
            Slice s = (Slice)o;
            if (length == s.length) {
                return (data == s.data && offset == s.offset)
                    || hashCode() == s.hashCode();
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            hashCode = hash(data,offset,length,0);
        }
        return hashCode;
    }

    private volatile int hashCode = 0;

    /**
     * Simple hash function used for internal data structures
     *
     * @param b - the data
     * @param off - the start offset in the data
     * @param len - the number of bytes to hash
     * @param seed - an initial hash value
     * @return a hash value on the data
     */
    public static int hash(byte[] b, int off, int len, int seed) {
        // Similar to murmur hash
        int m = 0xc6a4a793;
        int r = 24;
        int h = seed ^ (len * m);

        // Pick up four bytes at a time
        int limit = (off + len) - 3;
        while (off < limit) {
            int w = ( b[off++] & 0x0ff)
                  | ((b[off++] & 0x0ff) << 8)
                  | ((b[off++] & 0x0ff) << 16)
                  | ((b[off++] & 0x0ff) << 24);
            h += w;
            h *= m;
            h ^= (h >>> 16);
        }

        // Pick up remaining bytes
        switch ((limit + 3) - off) {
            case 3: h += (b[off+2] & 0x0ff) << 16; // FALLTHROUGH_INTENDED
            case 2: h += (b[off+1] & 0x0ff) << 8;  // FALLTHROUGH_INTENDED
            case 1: h += (b[off]   & 0x0ff);
                    h *= m;
                    h ^= (h >>> r);
                    break;
        }
        return h;
    }

}