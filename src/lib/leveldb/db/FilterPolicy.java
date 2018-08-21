package bsd.leveldb.db;

import java.util.Collection;

import bsd.leveldb.Slice;

/**
 * A database can be configured with a custom FilterPolicy object.
 *
 * This object is responsible for creating a small filter from a set of keys.
 *
 * These filters are stored in leveldb and are consulted automatically by leveldb
 * to decide whether or not to read some information from disk.
 *
 * In many cases, a filter can cut down the number of disk seeks
 * from a handful to a single disk seek per DB.get() call.
 *
 * Most people will want to use the builtin bloom filter support.
 */
public interface FilterPolicy {

    /**
     * Return the name of this policy.
     *
     * Note that if the filter encoding changes in an incompatible way,
     * the name returned by this method must be changed.
     * Otherwise, old incompatible filters may be passed to methods of this type.
     *
     * @return
     */
    String name();

    /**
     * Append a filter that summarizes keys[0,n-1] to src.
     *
     * keys[0,n-1] contains a list of keys (potentially with duplicates)
     * that are ordered according to the user supplied comparator.
     *
     * // Warning: do not change the initial contents of *dst.
     * // Instead, append the newly constructed filter to *dst.
     *
     * @param keys
     * @return
     */
    Slice createFilter(Collection<Slice> keys);

    /**
     * "filter" contains the data appended by a preceding call to CreateFilter() on this class.
     *
     * This method must return true if the key was in the list of keys passed to createFilter().
     * This method may return true or false if the key was not on the list, but it should aim to return false with a high probability.
     *
     * @param key
     * @param filter
     * @return
     */
    boolean keyMayMatch(Slice key, Slice filter);

}
