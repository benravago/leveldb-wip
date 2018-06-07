package bsd.leveldb.db;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

import bsd.leveldb.Slice;
import bsd.leveldb.io.ByteEncoder;
import static bsd.leveldb.db.DbFormat.*;

/**
 * BlockBuilder generates blocks where keys are prefix-compressed.
 *
 * When we store a key, we drop the prefix shared with the previous string.
 * This helps reduce the space requirement significantly.
 * Furthermore, once every K keys, we do not apply the prefix compression and store the entire key.
 * We call this a "restart point".
 * The tail end of the block stores the offsets of all of the restart points,
 * and can be used to do a binary search when looking for a particular key.
 * Values are stored as-is (without compression) immediately following the corresponding key.
 *
 * An entry for a particular key-value pair has the form:
 * <pre>
 *     shared_bytes: varint32
 *     unshared_bytes: varint32
 *     value_length: varint32
 *     key_delta: char[unshared_bytes]
 *     value: char[value_length]
 * </pre>
 * shared_bytes == 0 for restart points.
 *
 * The trailer of the block has the form:
 * <pre>
 *     restarts: uint32[num_restarts]
 *     num_restarts: uint32
 * </pre>
 * restarts[i] contains the offset within the block of the ith restart point.
 */
class BlockBuilder {

    // from Options
    int blockRestartInterval;
    Comparator<InternalKey> comparator;

    ByteEncoder buffer;     // Destination buffer
    List<Integer> restarts; // Restart points
    int counter;            // Number of entries emitted since restart
    boolean finished;       // Has Finish() been called?
    InternalKey lastKey;
    byte[] lastKeyData;

    BlockBuilder(int blockRestartInterval, Comparator<InternalKey> comparator) {
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;
        assert (blockRestartInterval >= 1);
        buffer = new ByteEncoder();
        restarts = new ArrayList<>();
        reset();
    }

    /**
     * Reset the contents as if the BlockBuilder was just constructed.
     */
    final void reset() {
        buffer.reset();
        restarts.clear();
        restarts.add(0);  // First restart point is at offset 0
        counter = 0;
        finished = false;
        lastKey = null;
        lastKeyData = new byte[0];
    }

    /**
     * Add a key/value pair.
     *
     * REQUIRES: Finish() has not been called since the last call to Reset().
     * REQUIRES: key is larger than any previously added key
     *
     * @param key
     * @param value
     */
    void add(InternalKey key, Slice value) {
        // Slice last_key_piece(last_key_);
        assert (!finished);
        assert (counter <= blockRestartInterval);
        assert (buffer.isEmpty() // No values yet?
            || comparator.compare(key,lastKey) > 0);  // else ensure monotonically increasing

        byte[] keyData = (key.sequence_type != -1)
            ? appendInternalKey(key) : key.userKey.data;

        int shared = 0;
        if (counter < blockRestartInterval) {
            // See how much sharing to do with previous string
            shared = commonPrefix(keyData,lastKeyData);
        } else {
            // Restart compression
            restarts.add(buffer.size());
            counter = 0;
        }
        int nonShared = keyData.length - shared;

        // Add "<shared><non_shared><value_size>" to buffer_
        buffer.putVarint32(shared);
        buffer.putVarint32(nonShared);
        buffer.putVarint32(value.length);

        // Add string delta to buffer_ followed by value
        buffer.write( keyData, shared, nonShared );
        if (value.length > 0) {
            buffer.write( value.data, value.offset, value.length );
        }
        // Update state
        lastKeyData = keyData;
        lastKey = key;
        counter++;
    }

    static int commonPrefix(byte[] a, byte[] b) {
        int k = a.length < b.length ? a.length : b.length;
        for (int i = 0; i < k; i++) {
            if (a[i] != b[i]) return i;
        }
        return 0;
    }

    /**
     * Finish building the block and return a slice that refers to the block contents.
     * The returned slice will remain valid for the lifetime of this builder or until Reset() is called.
     */
    Slice finish() {
        // Append restart array
        for (int i : restarts) {
            buffer.putFixed32(i);
        }
        buffer.putFixed32(restarts.size());
        finished = true;
        return buffer.toSlice();
    }

    static final int sizeof_uint32_t = 4;

    /**
     * Returns an estimate of the current (uncompressed) size of the block we are building.
     */
    int currentSizeEstimate() {
        return (buffer.size() +                       // Raw data buffer
               (restarts.size() * sizeof_uint32_t) +  // Restart array
               (sizeof_uint32_t) );                   // Restart array length
    }

    /**
     * Return true iff no entries have been added since the last Reset().
     */
    boolean isEmpty() {
        return buffer.isEmpty();
    }

}
