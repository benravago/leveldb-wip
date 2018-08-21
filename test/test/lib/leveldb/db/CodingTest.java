package lib.leveldb.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import lib.util.Varint;

import lib.leveldb.Slice;
import lib.leveldb.io.ByteDecoder;
import lib.leveldb.io.ByteEncoder;

public class CodingTest {

    static ByteDecoder data(ByteEncoder s) {
        return new ByteDecoder().wrap(s.toByteArray());
    }

    static byte[] b(int ... a) {
        byte[] b = new byte[a.length];
        for (int i = 0; i < a.length; i++) b[i] = (byte)a[i];
        return b;
    }

    @Test
    public void Coding_Fixed32() {
        ByteEncoder s = new ByteEncoder();
        for (int v = 0; v < 100000; v++) {
            s.putFixed32(v);
        }
        ByteDecoder p = data(s);
        for (int v = 0; v < 100000; v++) {
            int actual = p.getFixed32();
            assertEquals(v, actual);
        }
    }

    @Test
    public void Coding_Fixed64() {
        ByteEncoder s = new ByteEncoder(); // std::string s;
        for (int power = 0; power <= 63; power++) {
            long v = (1) << power;
            s.putFixed64(v - 1);
            s.putFixed64(v + 0);
            s.putFixed64(v + 1);
        }
        ByteDecoder p = data(s);
        for (int power = 0; power <= 63; power++) {
            long v = (1) << power;
            long actual;
            actual = p.getFixed64();
            assertEquals(v-1, actual);
            actual = p.getFixed64();
            assertEquals(v+0, actual);
            actual = p.getFixed64();
            assertEquals(v+1, actual);
        }
    }

    // Test that encoding routines generate little-endian encodings

    @Test
    public void Coding_EncodingOutput() {
        ByteEncoder src = new ByteEncoder();
        ByteDecoder dst;

        src.putFixed32(0x04030201);
        dst = data(src);
        assertEquals(4, src.size());
        byte[] p = dst.getBytes(4);
        assertEquals(0x01, p[0]);
        assertEquals(0x02, p[1]);
        assertEquals(0x03, p[2]);
        assertEquals(0x04, p[3]);

        src.clear();
        src.putFixed64(0x0807060504030201L);
        dst = data(src);
        assertEquals(8, src.size());
        p = dst.getBytes(8);
        assertEquals(0x01, p[0]);
        assertEquals(0x02, p[1]);
        assertEquals(0x03, p[2]);
        assertEquals(0x04, p[3]);
        assertEquals(0x05, p[4]);
        assertEquals(0x06, p[5]);
        assertEquals(0x07, p[6]);
        assertEquals(0x08, p[7]);
    }

    @Test
    public void Coding_Varint32() {
        ByteEncoder s = new ByteEncoder();
        for (int i = 0; i < (32 * 32); i++) {
            int v = (i / 32) << (i % 32);
            s.putVarint32(v);
        }
        ByteDecoder p = data(s);
        for (int i = 0; i < (32 * 32); i++) {
            int expected = (i / 32) << (i % 32);
            int actual;
            actual = p.getVarint32();
            assertEquals(expected, actual);
        }
        assertEquals(0,p.remaining());
    }

    @Test
    public void Coding_Varint64() {
        // Construct the list of values to check
        List<Long> values = new ArrayList<>();
        // Some special values
        values.add(0L);
        values.add(100L);
        // values.add(~0L);
        // values.add(~(0L) - 1);
        for (int k = 0; k < 63; k++) {
            // Test values near powers of two
            long power = 1L << k;
            values.add(power);
            values.add(power-1);
            values.add(power+1);
        }

        ByteEncoder s = new ByteEncoder();
        for (int i = 0; i < values.size(); i++) {
            s.putVarint64(values.get(i));
        }
        ByteDecoder p = data(s);
        for (int i = 0; i < values.size(); i++) {
            long actual;
            actual = p.getVarint64();
            assertEquals(values.get(i).longValue(), actual);
        }
        assertEquals(0,p.remaining());
    }

// TEST(Coding, Varint32Overflow) {
//   uint32_t result;
//   std::string input("\x81\x82\x83\x84\x85\x11");
//   ASSERT_TRUE(GetVarint32Ptr(input.data(), input.data() + input.size(), &result)
//               == NULL);
// }

// TEST(Coding, Varint32Truncation) {
//   uint32_t large_value = (1u << 31) + 100;
//   std::string s;
//   PutVarint32(&s, large_value);
//   uint32_t result;
//   for (size_t len = 0; len < s.size() - 1; len++) {
//     assertTrue(GetVarint32Ptr(s.data(), s.data() + len, &result) == NULL);
//   }
//   assertTrue(GetVarint32Ptr(s.data(), s.data() + s.size(), &result) != NULL);
//   assertEquals(large_value, result);
// }

    @Test
    public void Coding_Varint64Overflow() {
        byte[] input = b(0x81,0x82,0x83,0x84,0x85,0x81,0x82,0x83,0x84,0x85,0x11);
        TestUtil.expect(AssertionError.class, () -> { Varint.load(new long[1], input, 0); });
    }

// TEST(Coding, Varint64Overflow) {
//   uint64_t result;
//   std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
//   ASSERT_TRUE(GetVarint64Ptr(input.data(), input.data() + input.size(), &result)
//               == NULL);
// }

// TEST(Coding, Varint64Truncation) {
//   uint64_t large_value = (1ull << 63) + 100ull;
//   std::string s;
//   PutVarint64(&s, large_value);
//   uint64_t result;
//   for (size_t len = 0; len < s.size() - 1; len++) {
//     assertTrue(GetVarint64Ptr(s.data(), s.data() + len, &result) == NULL);
//   }
//   assertTrue(GetVarint64Ptr(s.data(), s.data() + s.size(), &result) != NULL);
//   assertEquals(large_value, result);
// }

    static Slice slice(String s) { return new Slice(s.getBytes()); }
    static String toString(Slice s) { return new String(s.data,s.offset,s.length); }

    @Test
    public void Coding_Strings() {
        char[] x = new char[200];
        Arrays.fill(x,'x');
        String x200 = new String(x);

        ByteEncoder s = new ByteEncoder();
        s.putLengthPrefixedSlice(slice(""));
        s.putLengthPrefixedSlice(slice("foo"));
        s.putLengthPrefixedSlice(slice("bar"));
        s.putLengthPrefixedSlice(slice(x200));

        ByteDecoder input = data(s);
        Slice v;
        v = input.getLengthPrefixedSlice();
        assertEquals("", toString(v));
        v = input.getLengthPrefixedSlice();
        assertEquals("foo", toString(v));
        v = input.getLengthPrefixedSlice();
        assertEquals("bar", toString(v));
        v = input.getLengthPrefixedSlice();
        assertEquals(x200, toString(v));

        assertEquals(0,input.remaining());
    }

}
