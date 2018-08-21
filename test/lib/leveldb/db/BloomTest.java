package bsd.leveldb.db;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import bsd.leveldb.Slice;
import static bsd.leveldb.io.ByteEncoder.*;

import static bsd.leveldb.db.TestUtil.*;

public class BloomTest {

    static final int kVerbose = Integer.getInteger("bloom_test.verbose",1);

    static Slice key(int i) {
        byte[] buffer = new byte[4];
        encodeFixed32(i, buffer, 0);
        return new Slice(buffer);
    }

    FilterPolicy policy = new BloomFilterPolicy(10);
    Slice filter = null;
    List<Slice> keys = new ArrayList<>();

    void reset() {
        keys.clear();
        filter = null;
    }

    void add(Slice s) {
        keys.add(s);
    }

    void build() {
        filter = policy.createFilter(keys);
        keys.clear();
        if (kVerbose >= 2) dumpFilter();
    }

    int filterSize() {
        return filter != null ? filter.length : -1;
    }

    void dumpFilter() { // TODO:
//     fprintf(stderr, "F(");
//     for (size_t i = 0; i+1 < filter_.size(); i++) {
//       const unsigned int c = static_cast<unsigned int>(filter_[i]);
//       for (int j = 0; j < 8; j++) {
//         fprintf(stderr, "%c", (c & (1 <<j)) ? '1' : '.');
//       }
//     }
//     fprintf(stderr, ")\n");
    }

    boolean matches(Slice s) {
        if (!keys.isEmpty()) {
            build();
        }
        return policy.keyMayMatch(s, filter);
    }
    boolean matches(String s) {
        return matches(s(s));
    }

    double falsePositiveRate() {
        int result = 0;
        for (int i = 0; i < 10000; i++) {
            if (matches(key(i + 1000000000))) {
                result++;
            }
        }
        return result / 10000.0;
    }

    @Test
    public void BloomTest_EmptyFilter() {
        assertTrue(! matches("hello"));
        assertTrue(! matches("world"));
    }

    @Test
    public void BloomTest_Small() {
        add(s("hello"));
        add(s("world"));
        assertTrue(matches("hello"));
        assertTrue(matches("world"));
        assertTrue(! matches("x"));
        assertTrue(! matches("foo"));
    }

    static int nextLength(int length) {
        if (length < 10) {
            length += 1;
        } else if (length < 100) {
            length += 10;
        } else if (length < 1000) {
            length += 100;
        } else {
            length += 1000;
        }
        return length;
    }

    @Test
    public void BloomTest_VaryingLengths() {

        // Count number of filters that significantly exceed the false positive rate
        int mediocreFilters = 0;
        int goodFilters = 0;

        for (int length = 1; length <= 10000; length = nextLength(length)) {
            reset();
            for (int i = 0; i < length; i++) {
                add(key(i));
            }
            build();

            assertTrue(""+length, filterSize() <= ((length * 10 / 8) + 40) );

            // All added keys must match
            for (int i = 0; i < length; i++) {
                assertTrue("Length "+length+"; key "+i, matches(key(i)));
            }

            // Check false positive rate
            double rate = falsePositiveRate();
            if (kVerbose >= 1) {
                fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                                rate*100.0, length, filterSize() );
            }
            assertTrue(rate <= 0.02);   // Must not be over 2%
            if (rate > 0.0125) mediocreFilters++;  // Allowed, but not too often
            else goodFilters++;
        }
        if (kVerbose >= 1) {
            fprintf(stderr, "Filters: %d good, %d mediocre\n",
                            goodFilters, mediocreFilters );
        }
        assertTrue(mediocreFilters <= goodFilters/5);
    }

    // Different bits-per-byte

}