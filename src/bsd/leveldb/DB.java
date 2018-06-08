package bsd.leveldb;

import java.io.Closeable;
import java.util.Map.Entry;

import bsd.leveldb.db.DbFactory;
import bsd.leveldb.db.FilterPolicy;

@bsd.LICENSE
/**
 * A DB is a persistent ordered map from keys to values.
 *
 * A DB is safe for concurrent access from multiple threads without any external synchronization.
 */
public abstract class DB implements Iterable<Entry<Slice,Slice>>, Closeable {

    public static final int kMajorVersion = 1; // commit 47cb9e2 on Oct 25
    public static final int kMinorVersion = 20;

    public static final Options defaultOptions = new Options();

    public final ReadOptions defaultReadOptions = new ReadOptions();
    public final WriteOptions defaultWriteOptions = new WriteOptions();

    /**
     * Open the database with the specified "name".
     * Returns a handle to the database on success or NULL on error.
     *
     * @param options - configuration parameters
     * @param name - database name
     * @return a database reference or null
     * @throws java.io.IOException
     */
    public static DB open(Options options, String name) {
        return DbFactory.openDB(options,name);
    }
    public static DB open(String name) {
        return open(defaultOptions,name);
    }

    @Override
    public abstract void close();

    /**
     * Set the database entry for "key" to "value".
     *
     * @param options - operational parameters
     * @param key - entry key
     * @param value - entry value
     */
    public abstract void put(WriteOptions options, Slice key, Slice value);

    public void put(Slice key, Slice value) { put(defaultWriteOptions,key,value); }

    /**
     * Remove the database entry (if any) for "key".
     * It is not an error if "key" did not exist in the database.
     *
     * @param options - operational parameters
     * @param key - key to remove
    */
    public abstract void delete(WriteOptions options, Slice key);

    public void delete(Slice key) { delete(defaultWriteOptions,key); }

    /**
     * Apply the specified updates to the database.
     *
     * @param options - operational parameters
     * @param updates - a set of update operations
     */
    public abstract void write(WriteOptions options, WriteBatch updates);

    public void write(WriteBatch updates) { write(defaultWriteOptions,updates); }

    /**
     * If the database contains an entry for "key" retrieve the corresponding value in *value.
     *
     * @param options - operational parameters
     * @param key - key to retrieve
     * @return the corresponding value or null if key not found
     */
    public abstract Slice get(ReadOptions options, Slice key);

    public Slice get(Slice key) { return get(defaultReadOptions,key); }

    /**
     * Return an iterator over the contents of the database.
     *
     * @param options - operational parameters
     * @return an iterator over the current key/value pairs in the database
     */
    public abstract Cursor<Slice,Slice> iterator(ReadOptions options);

    @Override
    public Cursor<Slice,Slice> iterator() { return iterator(defaultReadOptions); }

    /**
     * Return a handle to the current DB state.
     * Iterators created with this handle will all observe a stable snapshot of the current DB state.
     *
     * @return a Snapshot reference
     */
    public abstract Snapshot getSnapshot();

    /**
     * Release a previously acquired snapshot.
     * The caller must not use "snapshot" after this call.
     *
     * @param snapshot - a Snapshot reference
     */
    public abstract void releaseSnapshot(Snapshot snapshot);

    /**
     * DB implementations can export properties about their state via this method.
     *
     * If "property" is a valid property understood by this DB implementation, fills "*value" with its current value.
     *
     * @param property = one or more property names
     * @return a map of property name/value pairs
     */
    public abstract <T> T getProperty(String property);

    // Valid property names include:
    //
    //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
    //     where <N> is an ASCII representation of a level number (e.g. "0").
    //  "leveldb.stats" - returns a multi-line string that describes statistics
    //     about the internal operation of the DB.
    //  "leveldb.sstables" - returns a multi-line string that describes all
    //     of the sstables that make up the db contents.
    //  "leveldb.approximate-memory-usage" - returns the approximate number of
    //     bytes of memory in use by the DB.

    /**
     * For each i in [0,n-1], store in "sizes[i]",
     * the approximate file system space used by keys in "[range[i].start : range[i].limit]".
     *
     * Note that the returned sizes measure file system space usage,
     * so if the user data compresses by a factor of ten,
     * the returned sizes will be one-tenth the size of the corresponding user data size.
     *
     * The results may not include the sizes of recently written data.
     *
     * @param range - one or more key ranges where:
     *                  slice[0] = start, included in the range;
     *                  slice]1] = limit, not included in the range.
     *
     * @return the corresponding approximate sizes for each input range
     */
    public abstract long getApproximateSize(Slice start, Slice limit);

    /**
     * Compact the underlying storage for the key range [*begin,*end].
     *
     * In particular, deleted and overwritten versions are discarded,
     * and the data is rearranged to reduce the cost of operations needed to access the data.
     * This operation should typically only be invoked by users who understand the underlying implementation.
     *
     * begin==NULL is treated as a key before all keys in the database.
     * end==NULL is treated as a key after all keys in the database.
     * Therefore the following call will compact the entire database:
     *   db->CompactRange(NULL, NULL);
     *
     * @param begin - range start key
     * @param end - range end key
     */
    public abstract void compactRange(Slice begin, Slice end);

    /**
     * Destroy the contents of the specified database.
     * Be very careful using this method.
     *
     * @param options - operational options
     * @param name - database name
     */
    public static void destroyDB(Options options, String name) {
        DbFactory.destroyDB(options,name);
    }
    public static void destroyDB(String name) {
        destroyDB(defaultOptions,name);
    }

    /**
     * If a DB cannot be opened, you may attempt to call this method to resurrect as much of the contents of the database as possible.
     * Some data may be lost, so be careful when calling this function on a database that contains important information.
     *
     * @param options - operational options
     * @param name - database name
     */
    public static void repairDB(Options options, String name) {
        DbFactory.repairDB(options,name);
    }
    public static void repairDB(String name) {
        repairDB(defaultOptions,name);
    }

    /**
     * Create a new WriteBatch object
     * @return a WriteBatch object
     */
    public static WriteBatch newBatch() {
        return DbFactory.newWriteBatch();
    }

    /**
     * Create a new default FilterPolicy object
     * @return a (Bloom) FilterPolicy
     */
    public static FilterPolicy newBloomFilterPolicy() {
        return DbFactory.newBloomFilterPolicy(-1);
    }

    /**
     * Convert a String to a Slice
     *
     * @param s
     * @return
     */
    public static Slice slice(String s) {
        return new Slice(s.getBytes());
    }

}