package lib.leveldb.db;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import lib.leveldb.Slice;
import static lib.leveldb.db.DbFormat.*;

import static lib.leveldb.db.TestUtil.*;

public class DbFormatTest {

    static String toString(Slice s) { return new String(s.data,s.offset,s.length); }

    static InternalKeyComparator cmp = new InternalKeyComparator(new BytewiseComparator());

    static byte[] ikey(String userKey, long seq, int vt) {
        return ikey(s(userKey),seq,vt);
    }
    static byte[] ikey(Slice userKey, long seq, int vt) {
        byte[] encoded
            = appendInternalKey(internalKey(userKey, seq, vt));
        return encoded;
    }

    static byte[] shorten(byte[] s, byte[] l) {
        InternalKey start = parseInternalKey(s,0,s.length);
        InternalKey limit = parseInternalKey(l,0,l.length);
        InternalKey result = cmp.findShortestSeparator(start, limit);
        return appendInternalKey(result);
    }

    static byte[] shortSuccessor(byte[] s) {
        InternalKey result
            = cmp.findShortSuccessor(parseInternalKey(s,0,s.length));
        return appendInternalKey(result);
    }

    static void testKey(String key, long seq, int vt) {
        byte[] encoded = ikey(key, seq, vt);

        Slice in = new Slice(encoded);
        InternalKey decoded;

        assertNotNull(decoded = parseInternalKey(in.data,in.offset,in.length));
        assertEquals(key, toString(decoded.userKey));
        assertEquals(seq, sequenceNumber(decoded));
        assertEquals(vt, valueType(decoded));

        assertTrue(parseInternalKey("bar".getBytes(),0,3) == null);
    }

    @Test
    public void FormatTest_InternalKey_EncodeDecode() {
        String[] keys = {
            "", "k", "hello", "longggggggggggggggggggggg"
        };
        long[] seq = {
            1, 2, 3,
            (1L << 8) - 1, 1L << 8, (1L << 8) + 1,
            (1L << 16) - 1, 1L << 16, (1L << 16) + 1,
            (1L << 32) - 1, 1L << 32, (1L << 32) + 1
        };
        for (int k = 0; k < keys.length; k++) {
            for (int s = 0; s < seq.length; s++) {
                testKey(keys[k], seq[s], kTypeValue);
                testKey("hello", 1, kTypeDeletion);
            }
        }
    }

    @Test
    public void FormatTest_InternalKeyShortSeparator() {
        // When user keys are same
        assertArrayEquals(ikey("foo", 100, kTypeValue),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("foo", 99, kTypeValue)));
        assertArrayEquals(ikey("foo", 100, kTypeValue),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("foo", 101, kTypeValue)));
        assertArrayEquals(ikey("foo", 100, kTypeValue),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("foo", 100, kTypeValue)));
        assertArrayEquals(ikey("foo", 100, kTypeValue),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("foo", 100, kTypeDeletion)));

        // When user keys are misordered
        assertArrayEquals(ikey("foo", 100, kTypeValue),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("bar", 99, kTypeValue)));

        // When user keys are different, but correctly ordered
        assertArrayEquals(ikey("g", kMaxSequenceNumber, kValueTypeForSeek),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("hello", 200, kTypeValue)));

        // When start user key is prefix of limit user key
        assertArrayEquals(ikey("foo", 100, kTypeValue),
                  shorten(ikey("foo", 100, kTypeValue),
                          ikey("foobar", 200, kTypeValue)));

        // When limit user key is prefix of start user key
        assertArrayEquals(ikey("foobar", 100, kTypeValue),
                  shorten(ikey("foobar", 100, kTypeValue),
                          ikey("foo", 200, kTypeValue)));
    }

    static final Slice xFFxFF = new Slice(new byte[]{(byte)0xff,(byte)0xff}, 0, 2);

    @Test
    public void FormatTest_InternalKeyShortestSuccessor() {
        assertArrayEquals(ikey("g", kMaxSequenceNumber, kValueTypeForSeek),
           shortSuccessor(ikey("foo", 100, kTypeValue)));
        assertArrayEquals(ikey(xFFxFF, 100, kTypeValue),
           shortSuccessor(ikey(xFFxFF, 100, kTypeValue)));
    }

}
