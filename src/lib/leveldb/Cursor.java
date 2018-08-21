package bsd.leveldb;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map.Entry;

public interface Cursor<K,V> extends Iterator<Entry<K,V>>, Entry<K,V>, Closeable {

    /* Iterator<Entry<K,V>> */

    @Override default boolean hasNext() { return false; }
    @Override default Entry<K, V> next() { return null; }

    /* Entry<K,V> */

    @Override default K getKey() { return null; }
    @Override default V getValue() { return null; }
    @Override default V setValue(V value) { return null; }

    /* Closeable */

    @Override default void close() {}

}
