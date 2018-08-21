package bsd.leveldb.db;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import java.lang.reflect.Array;
import java.util.function.Supplier;

import bsd.leveldb.Slice;
import bsd.leveldb.Cursor;

final class Struct {

    static class Ref<T> { Ref(T t){ v=t; } T v; }

    static class Bool { boolean v = false; }
    static class SequenceNumber { long v = 0; }

    static long join(long hi, long lo) { return (hi << 32) | (lo & 0x0ffffffff); }
    static int[] split(long l) { return new int[]{ (int)(l >>> 32), (int)(l & 0x0ffffffff) }; }

    static <K,V> Entry<K,V> entry(K k, V v) {
        return new Entry<K,V>(){
            @Override public K getKey() { return k; }
            @Override public V getValue() { return v; }
            @Override public V setValue(V x) { return null; }
        };
    }

    static Slice slice(String s) {
        return new Slice(s.getBytes());
    }

    static Slice copy(Slice s) {
        byte[] b = new byte[s.length];
        System.arraycopy(s.data,s.offset, b,0,b.length);
        return new Slice(b,0,b.length);
    }

    static <T> T[] repeat(Supplier<T> s, int n) {
        T t = s.get();
        T[] a = (T[]) Array.newInstance(t.getClass(),n);
        a[0] = t;
        for (int i = 1; i < n; i++) a[i] = s.get();
        return a;
    }

    static <K,V> Cursor<K,V> cursor(Iterator<Entry<K,V>> iter) {
        return cursor(iter,()->{});
    }

    static <K,V> Cursor<K,V> cursor(Iterator<Entry<K,V>> iter, Runnable close) {
        return new Cursor<K,V>() {
            Entry<K,V> next;

            @Override public void close() {
                close.run();
            }
            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }
            @Override
            public Entry<K,V> next() {
                if (hasNext()) {
                    next = iter.next();
                    return this;
                }
                throw new NoSuchElementException();
            }
            @Override public K getKey() { return next.getKey(); }
            @Override public V getValue() { return next.getValue(); }
        };
    }
}
