package lib.leveldb.db;

import java.util.Collection;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import lib.leveldb.Slice;
import lib.leveldb.DB.FilterPolicy;
import lib.leveldb.io.ByteDecoder;
import lib.leveldb.io.ByteEncoder;

import static lib.leveldb.db.TestUtil.*;

public class FilterBlockTest {

    static String escapeString(Slice s) {
    	return DbUtil.string(s.data,s.offset,s.length).toString();
    }

    // For testing: emit an array with one hash value per key
    class TestHashFilter implements FilterPolicy {

        @Override
        public String name() {
            return "TestHashFilter";
        }

        @Override
        public Slice createFilter(Collection<Slice> keys) {
            byte[] dest = new byte[keys.size()*4];
            int destPos = 0;
            for (Slice key : keys) {
                int h = Slice.hash(key.data, key.offset, key.length, 1);
                ByteEncoder.encodeFixed32(h, dest, destPos);
                destPos += 4;
            }
            return new Slice(dest);
        }

        @Override
        public boolean keyMayMatch(Slice key, Slice filter) {
            ByteDecoder f = new ByteDecoder().wrap(filter);
            int h = Slice.hash(key.data, key.offset, key.length, 1);
            while (f.remaining() > 0) {
                if (h == f.getFixed32()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Test
    public void FilterBlockTest_EmptyBuilder() {
        FilterPolicy policy = new TestHashFilter();
        Filter.BlockBuilder builder = Filter.blockBuilder(policy);
        Slice block = builder.finish();
        assertEquals("\\000\\000\\000\\000\\013", escapeString(block));
        Filter reader = Filter.blockReader(policy, block);
        assertTrue(reader.keyMayMatch(0, s("foo")));
        assertTrue(reader.keyMayMatch(100000, s("foo")));
    }

    @Test
    public void FilterBlockTest_SingleChunk() {
        FilterPolicy policy = new TestHashFilter();
        Filter.BlockBuilder builder = Filter.blockBuilder(policy);
        builder.startBlock(100);
        builder.addKey(s("foo"));
        builder.addKey(s("bar"));
        builder.addKey(s("box"));
        builder.startBlock(200);
        builder.addKey(s("box"));
        builder.startBlock(300);
        builder.addKey(s("hello"));
        Slice block = builder.finish();
        Filter reader = Filter.blockReader(policy,block);
        assertTrue(reader.keyMayMatch(100, s("foo")));
        assertTrue(reader.keyMayMatch(100, s("bar")));
        assertTrue(reader.keyMayMatch(100, s("box")));
        assertTrue(reader.keyMayMatch(100, s("hello")));
        assertTrue(reader.keyMayMatch(100, s("foo")));
        assertTrue(! reader.keyMayMatch(100, s("missing")));
        assertTrue(! reader.keyMayMatch(100, s("other")));
    }

    @Test
    public void FilterBlockTest_MultiChunk() {
        FilterPolicy policy = new TestHashFilter();
        Filter.BlockBuilder builder = Filter.blockBuilder(policy);

        // First filter
        builder.startBlock(0);
        builder.addKey(s("foo"));
        builder.startBlock(2000);
        builder.addKey(s("bar"));

        // Second filter
        builder.startBlock(3100);
        builder.addKey(s("box"));

        // Third filter is empty

        // Last filter
        builder.startBlock(9000);
        builder.addKey(s("box"));
        builder.addKey(s("hello"));

        Slice block = builder.finish();
        Filter reader = Filter.blockReader(policy, block);

        // Check first filter
        assertTrue(reader.keyMayMatch(0, s("foo")));
        assertTrue(reader.keyMayMatch(2000, s("bar")));
        assertTrue(! reader.keyMayMatch(0, s("box")));
        assertTrue(! reader.keyMayMatch(0, s("hello")));

        // Check second filter
        assertTrue(reader.keyMayMatch(3100, s("box")));
        assertTrue(! reader.keyMayMatch(3100, s("foo")));
        assertTrue(! reader.keyMayMatch(3100, s("bar")));
        assertTrue(! reader.keyMayMatch(3100, s("hello")));

        // Check third filter (empty)
        assertTrue(! reader.keyMayMatch(4100, s("foo")));
        assertTrue(! reader.keyMayMatch(4100, s("bar")));
        assertTrue(! reader.keyMayMatch(4100, s("box")));
        assertTrue(! reader.keyMayMatch(4100, s("hello")));

        // Check last filter
        assertTrue(reader.keyMayMatch(9000, s("box")));
        assertTrue(reader.keyMayMatch(9000, s("hello")));
        assertTrue(! reader.keyMayMatch(9000, s("foo")));
        assertTrue(! reader.keyMayMatch(9000, s("bar")));
    }

}