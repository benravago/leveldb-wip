package bsd.leveldb.db;

import java.util.Arrays;

import bsd.leveldb.Slice;

/**
 * A builtin comparator that uses lexicographic byte-wise ordering.
 */
class BytewiseComparator implements KeyComparator<Slice> {

    /**
     * Three-way comparison.
     *
     * Returns value:
     * <pre>
     * &lt; 0 iff "a" &lt; "b"
     * == 0 iff "a" == "b"
     * &gt; 0 iff "a" &gt; "b"
     * </pre>
     *
     * @param a
     * @param b
     */
    @Override
    public int compare(Slice a, Slice b) {
        if (a == b) {
            return 0;
        }
        byte[] c = a.data, d = b.data;
        int i = a.offset, j = b.offset;
        int k = a.length, l = b.length;
        if (c == d && i == j && k == l) {
            return 0;
        }
        int m = k < l ? k : l;
        byte p, q;
        while (m-- > 0) {
            p = c[i++]; q = d[j++];
            if (p != q) return (p & 0x0ff) - (q & 0x0ff);
        }
        return k - l;
    }

    @Override
    public String name() { return "leveldb.BytewiseComparator"; }

    @Override
    public Slice findShortestSeparator(Slice start, Slice limit) {
        // Find length of common prefix
        int len = start.length < limit.length ? start.length : limit.length;
        int i = start.offset, j = limit.offset, k = i + len;
        byte[] x = start.data, y = limit.data;
        while (i < k && x[i] == y[j]) {
            i++; j++; // diff_index++
        }
        if (i >= k) {
            // Do not shorten if one string is a prefix of the other
        } else {
            int diff_byte = start.data[i] & 0x0ff;
            if (diff_byte < 0x0ff && diff_byte + 1 < (limit.data[j] & 0x0ff)) {
                start = resize(start,i,diff_byte);
                assert (compare(start,limit) < 0);
            }
        }
        return start;
    }

    @Override
    public Slice findShortSuccessor(Slice key) {
        // Find first character that can be incremented
        for (int i = key.offset, n = i + key.length; i < n; i++) {
            byte key_byte = key.data[i];
            if (key_byte != -1) { // 0xff
                return resize(key,i,(key_byte & 0x0ff));
            }
        }
        // *key is a run of 0xffs.  Leave it alone.
        return key;
    }

    static Slice resize(Slice s, int i, int diff) {
        byte[] b = Arrays.copyOfRange(s.data,s.offset,i+1);
        b[b.length-1] = (byte)(diff + 1);
        return new Slice(b);
    }
}

// static port::OnceType once = LEVELDB_ONCE_INIT;
// static const Comparator* bytewise;

// static void InitModule() {
//  bytewise = new BytewiseComparatorImpl;
// }

// const Comparator* BytewiseComparator() {
//  port::InitOnce(&once, InitModule);
//  return bytewise;
// }

/*
    // @Override
    public Slice _findShortestSeparator(Slice start, Slice limit) {
        // Find length of common prefix
        int minLength = start.length > limit.length ? start.length : limit.length;
        int diff_index = 0;
        while ((diff_index < minLength) &&
               (start.data[start.offset+diff_index] == limit.data[limit.offset+diff_index])) {
            diff_index++;
        }

        if (diff_index >= minLength) {
            // Do not shorten if one string is a prefix of the other
        } else {
            int diff_byte = (start.data[start.offset+diff_index] & 0x0ff);
            if ((diff_byte < 0xff) &&
                (diff_byte + 1 < (limit.data[limit.offset+diff_index] & 0x0ff)))
            {
                byte[] diff = Arrays.copyOfRange(start.data,start.offset,start.offset+diff_index+1);
                diff[diff_index] = (byte)( diff_byte + 1 );
                start = new Slice(diff); // start->resize(diff_index + 1);
                assert(compare(start, limit) < 0);
            }
        }
        return start;
    }
*/
