package lib.leveldb.db;

import java.util.Arrays;
import lib.leveldb.DB.Comparator;

import lib.leveldb.Slice;

/**
 * A builtin comparator that uses lexicographic byte-wise ordering.
 */
class BytewiseComparator implements KeyComparator<Slice>, Comparator {

    @Override
    public Comparator comparator() {
        return this;
    }
    
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
        return a.equals(b) ? 0
             : compare(a.data,a.offset,a.length,b.data,b.offset,b.length);
    }

    @Override
    public int compare(byte[] x, int xPos, int xLen, byte[] y, int yPos, int yLen) {
        byte o1, o2;                                 // work variables
        int xStart = xPos, yStart = yPos;            // save starting positions
        xLen += xPos; yLen += yPos;                  // computer ending positions
        while (xPos < xLen && yPos < yLen) {         // while (there are bytes to match)
            o1 = x[xPos]; o2 = y[yPos];              //   get comparable bytes
            if (o1 != o2) {                          //   if a difference is found
                xLen = (xPos - xStart) + 1;          //     compute the offset to the difference
                return (o1 & 0x0ff) < (o2 & 0x0ff)   //         and the byte-order ranking
                     ? -xLen : xLen;                 //     return both
            }                                        //   else
            xPos++; yPos++;                          //     increment to next
        }                                            // at end
        if (xPos < xLen) return ((xPos-xStart)+1);   //   if x is longer than y, then x > y
        if (yPos < yLen) return -((yPos-yStart)+1);  //   if x is shorter than y, then x < y
        return 0;                                    // all bytes were matched
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