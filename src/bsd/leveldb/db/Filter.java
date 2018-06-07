package bsd.leveldb.db;

import java.util.List;
import java.util.ArrayList;

import bsd.leveldb.Slice;
import bsd.leveldb.io.ByteDecoder;
import bsd.leveldb.io.ByteEncoder;
import static bsd.leveldb.db.DbFormat.*;

class Filter {

    FilterPolicy policy;
    Slice[] data = new Slice[0];   // filter data array
    int baseLg;                    // Encoding parameter (see kFilterBaseLg in .cc file)

    boolean keyMayMatch(int blockOffset, Slice key) {
        if (data.length < 1) {
            return true; // no filters
        }
        int index = blockOffset >> baseLg;
        if (index < data.length) {
            Slice filter = data[index];
            if (filter != null) {
                return policy.keyMayMatch(key, filter);
            } else {
                // Empty filters do not match any keys
                return false;
            }
        }
        return true;  // Errors are treated as potential matches
    }


    // A filter block is stored near the end of a Table file.
    // It contains filters (e.g., bloom filters) for all data blocks
    // in the table combined into a single filter block.

    static final byte[] key = "filter.".getBytes();

    // See doc/table_format.md for an explanation of the filter block format.

    // Generate new filter every 2KB of data
    static final int kFilterBaseLg = 11;
    static final int kFilterBase = 1 << kFilterBaseLg;

    static BlockBuilder blockBuilder(FilterPolicy policy) {
        return new BlockBuilder(policy);
    }

    static Filter blockReader(FilterPolicy policy, Slice contents) {
        Filter f = new Filter();
        if (policy == null || contents.length < 5) {
            return f; // 1 byte for base_lg_ and 4 for start of offset array
        }

        ByteDecoder d = new ByteDecoder().wrap(contents);
        d.position(-5);
        int offsetEnd = d.position();
        int offsetStart = d.getFixed32();
        int baseLg = d.getFixed8();

        if (offsetEnd <= offsetStart) return f;
        int filterCount = (offsetEnd - offsetStart) / sizeof_uint32_t;
        Slice[] data = new Slice[filterCount];

        d.position(offsetStart);
        int previous = d.getFixed32();
        for (int i = 0; i < data.length; i++) {
            int next = d.getFixed32();
            int off = contents.offset + previous;
            int len = next - previous;
            data[i] = len > 0 ? new Slice(contents.data,off,len) : null;
            previous = next;
        }

        f.policy = policy;
        f.baseLg = baseLg;
        f.data = data;
        return f;
    }

    /**
     * A FilterBlockBuilder is used to construct all of the filters for a particular Table.
     * It generates a single string which is stored as a special block in the Table.
     *
     * The sequence of calls to FilterBlockBuilder must match the regexp:
     * <pre>
     *     (StartBlock AddKey*)* Finish
     * </pre>
     *
     * A filter block is stored near the end of a Table file.
     * It contains filters (e.g., bloom filters) for all data blocks in the table
     * combined into a single filter block.
     *
     * See doc/table_format.md for an explanation of the filter block format.
     */
    static class BlockBuilder {

        final FilterPolicy policy;
        final List<Slice> keys;          // Flattened key contents
        final ByteEncoder result;        // Filter data computed so far
        final List<Integer> filterOffsets;

        BlockBuilder(FilterPolicy policy) {
            this.policy = policy;
            filterOffsets = new ArrayList<>();
            keys = new ArrayList<>();
            result = new ByteEncoder();
        }

        Slice name() {
            return new Slice(( "filter." + policy.name() ).getBytes());
        }

        void startBlock(long blockOffset) {
            long filterIndex = (blockOffset / kFilterBase);
            assert (filterIndex >= filterOffsets.size());
            while (filterIndex > filterOffsets.size()) {
                generateFilter();
            }
        }

        void addKey(Slice key) {
            keys.add(key);
        }

        Slice finish() {
            if (!keys.isEmpty()) {
                generateFilter();
            }

            // Append array of per-filter offsets
            int arrayOffset = result.size();
            for (int i = 0; i < filterOffsets.size(); i++) {
                result.putFixed32(filterOffsets.get(i));
            }

            result.putFixed32(arrayOffset);
            result.putFixed8(kFilterBaseLg);  // Save encoding parameter in result
            return result.toSlice();
        }

        void generateFilter() {
            int numKeys = keys.size();
            if (numKeys == 0) {
                // Fast path if there are no keys for this filter
                filterOffsets.add(result.size());
                return;
            }

            // Generate filter for current set of keys and append to result_.
            filterOffsets.add(result.size());
            Slice tmp = policy.createFilter(keys);
            result.putSlice(tmp);

            keys.clear();
        }
    }
}
