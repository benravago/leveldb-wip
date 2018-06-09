package bsd.leveldb.io;

import java.util.Iterator;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public interface Iteration {

    static <T> Iterable<T> of(Iterator<T> iterator) {
        return () -> iterator != null ? iterator : none;
    }

    static <T> Iterable<T> of(Enumeration<T> enumeration) {
        return () -> enumeration != null ? enumeration.asIterator() : none;
    }

    static final Iterator none = new Iterator(){
        @Override public boolean hasNext() { return false; }
        @Override public Object next() { return null; }
    };

    static final Iterable empty = () -> none;

    static <T> Iterable<T> of(T[] array) {
        return () -> elements(array);
    }

    static <T> Iterator<T> elements(T[] array) {
        return new Iterator<T>() {
            @Override public boolean hasNext() { return i < array.length; }
            @Override public T next() { return array[i++]; }
            int i = 0;
        };
    }

    static <T> Iterator<T> from(Supplier<T> src) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                if (next == null) {
                    next = src.get();
                }
                return next != null;
            }
            @Override
            public T next() {
                if (hasNext()) {
                    T item = next;
                    next = null;
                    return item;
                }
                throw new NoSuchElementException();
            }
            private T next = null;
        };
    }

}