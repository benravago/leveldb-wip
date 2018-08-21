package bsd.leveldb.db;

import java.util.Comparator;

/**
 * A Comparator object provides a total order across slices that are used as keys in an sstable or a database.
 *
 * A Comparator implementation must be thread-safe since leveldb may invoke its methods concurrently from multiple threads.
 *
 * @param <T>
 */
public interface KeyComparator<T> extends Comparator<T>{

    /**
     * The name of the comparator.
     *
     * Used to check for comparator mismatches (i.e., a DB created with one comparator is accessed using a different comparator.
     *
     * The client of this package should switch to a new name
     * whenever the comparator implementation changes
     * in a way that will cause the relative ordering of any two keys to change.
     *
     * Names starting with "leveldb." are reserved and should not be used by any clients of this package.
     *
     * @return
     */
    String name();

    // Advanced functions:
    // these are used to reduce the space requirements for internal data structures like index blocks.

    /**
     * If *start &lt; limit, changes *start to a short string in [start,limit].
     *
     * Simple comparator implementations may return with *start unchanged,
     * i.e., an implementation of this method that does nothing is correct.
     *
     * @param start
     * @param limit
     * @return
     */
    default T findShortestSeparator(T start, T limit) { return start; }

    /**
     * Changes *key to a short string >= *key.
     *
     * Simple comparator implementations may return with *key unchanged,
     * i.e., an implementation of this method that does nothing is correct.
     *
     * @param key
     * @return
     */
    default T findShortSuccessor(T key) { return key; };

}
