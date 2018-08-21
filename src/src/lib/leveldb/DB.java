package lib.leveldb;

import java.util.Collection;
import java.util.Map;

import java.io.Closeable;

import java.nio.file.Path;
import java.nio.file.OpenOption;

/**
 * A DB is a persistent ordered map from keys to values.
 * <p>
 * A DB is safe for concurrent access from multiple threads without any external synchronization.
 */
public interface DB extends Iterable<Map.Entry<Slice,Slice>>, Closeable {

    static Builder builder() {
        return Factory.builder();
    }

    interface Builder {

        Builder reuseLogs(boolean b);            // bool reuse_logs;
        Builder paranoidChecks(boolean b);       // bool paranoid_checks;

        Builder maxFileSize(int i);              // size_t max_file_size;
        Builder maxOpenFiles(int i);             // int max_open_files;
        Builder writeBufferSize(int i);          // size_t write_buffer_size;
        Builder blockCacheSize(int i);           // Cache* block_cache;
        Builder blockSize(int i);                // size_t block_size;
        Builder blockRestartInterval(int i);     // int block_restart_interval;

        Builder comparator(Comparator c);        // const Comparator* comparator;
        Builder env(Env e);                      // Env* env;
                                                 // Cache* block_cache;
        Builder compression(CompressionType c);  // CompressionType compression;
        Builder filterPolicy(FilterPolicy f);    // const FilterPolicy* filter_policy;

        /**
         * Open the database at the specified path.
         * The DB "name" is the last directory name of the path.
         *
         * @param  path  to the DB directory.
         * @param  options  open options.
         * @return a DB reference.
         */
        DB open(Path path, OpenOption ... options);  // CREATE, CREATE_NEW

        void destroy(Path path);
        void repair(Path path);

        FilterPolicy newFilterPolicy();         // return default FilterPolicy<Slice>
        Comparator newComparator();             // return default Comparator<Slice>
        Env newEnv();                           // return default Env()
    }

    interface Comparator {
        int compare(byte[] x, int xPos, int xLen, byte[] y, int yPos, int yLen);
    }

    interface FilterPolicy {
        String name();
        Slice createFilter(Collection<Slice> keys);
        boolean keyMayMatch(Slice key, Slice filter);
    }

    enum CompressionType {

        NoCompression(0x00),
        SnappyCompression(0x01);

        CompressionType(int c) {
            code = c;
        }
        public final int code;
    }

    // Map<String,String> getProperties();
    <T> T getProperty(String key);

    interface Snapshot {}

    Snapshot getSnapshot();
    void releaseSnapshot(Snapshot snapshot);

    @Override
    default Cursor<Slice,Slice> iterator() {
        return iterator(null);
    }
    default Cursor<Slice,Slice> iterator(Snapshot snapshot) {
        return iterator(snapshot,true,false);
    }

    Cursor<Slice,Slice> iterator(Snapshot snapshot, boolean fillCache, boolean verifyChecksums);

    /**
     * If the database contains an entry for "key", return the corresponding value.
     * If there is no entry for "key", return null.
     * @param key
     * @return
     */
    default Slice get(Slice key) {
        return get(key,null);
    }
    default Slice get(Slice key, Snapshot snapshot) {
        return get(key,snapshot,true,false);
    }

    Slice get(Slice key, Snapshot snapshot, boolean fillCache, boolean verifyChecksums);

    interface WriteBatch extends Iterable<Map.Entry<Slice,Slice>> {
        WriteBatch put(Slice key, Slice value);
        WriteBatch delete(Slice key);
        WriteBatch clear();
        void apply(boolean sync);
    }

    WriteBatch batch();

    /**
     * Set the database entry for "key" to "value".
     * @param key
     * @param value
     */
    default void put(Slice key, Slice value) {
        put(key,value,false);
    }
    default void put(Slice key, Slice value, boolean sync) {
        batch().put(key,value).apply(sync);
    }

    /**
     * Remove the database entry (if any) for "key".
     * @param key
     */
    default void delete(Slice key) {
        delete(key,false);
    }
    default void delete(Slice key, boolean sync) {
        batch().delete(key).apply(sync);
    }

    long getApproximateSize(Slice begin, Slice end);
    void compact(Slice begin, Slice end);

    @Override
    void close();
}
