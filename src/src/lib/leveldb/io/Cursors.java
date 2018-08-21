package lib.leveldb.io;

import java.util.Iterator;
import java.util.Map.Entry;

import lib.leveldb.Cursor;

public interface Cursors {

    static <K,V> Cursor<K,V> wrap(Iterator<Entry<K,V>> iter) {
        return wrap(iter,()->{});
    }

    static <K,V> Cursor<K,V> wrap(Iterator<Entry<K,V>> iter, Runnable close) {
        return new Cursor<K,V>() {
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }
            @Override
            public Entry<K,V> next() {
                next = iter.next();
                return this;
            }

            Entry<K,V> next;

            @Override public K getKey() { return next.getKey(); }
            @Override public V getValue() { return next.getValue(); }

            @Override public void close() { close.run(); }
        };
    }

    static <K,V> Entry<K,V> entry(K k, V v) {
        return new Entry<K,V>(){
            @Override public K getKey() { return k; }
            @Override public V getValue() { return v; }
            @Override public V setValue(V x) { return x; }
        };
    }

}
