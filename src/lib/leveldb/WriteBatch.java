package bsd.leveldb;

import java.util.Iterator;
import java.util.Map.Entry;

/**
 * WriteBatch holds a collection of updates to apply atomically to a DB.
 *
 * The updates are applied in the order in which they are added to the WriteBatch.
 * For example, the value of "key" will be "v3" after the following batch is written:
 *
 *    batch.put("key", "v1");
 *    batch.delete("key");
 *    batch.put("key", "v2");
 *    batch.put("key", "v3");
 *
 * Multiple threads can invoke const methods on a WriteBatch without external synchronization,
 * but if any of the threads may call a non-final method,
 * all threads accessing the same WriteBatch must use external synchronization.
 */
public interface WriteBatch extends Iterable<Entry<Slice,Slice>> {

    /**
     * Store the mapping "key->value" in the database.
     *
     * @param key
     * @param value
     */
    void put(Slice key, Slice value);

    /**
     * If the database contains a mapping for "key", erase it.
     * Else do nothing.
     *
     * @param key
     */
    void delete(Slice key);

    /**
     * Clear all updates buffered in this batch.
     */
    void clear();

    /**
     * Support for iterating over the contents of a batch.
     */
    @Override
    Iterator<Entry<Slice,Slice>> iterator();

}