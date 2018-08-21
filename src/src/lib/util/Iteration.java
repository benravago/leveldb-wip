package lib.util;

import java.util.Iterator;
import java.util.Enumeration;
import java.util.NoSuchElementException;

public interface Iteration {

    static <T> Iterable<T> each(Iterator<T> iterator) {
        return () -> iterator;
    }

    static <T> Iterable<T> each(Enumeration<T> enumeration) {
        return each(enumeration.asIterator());
    }

    @SuppressWarnings("rawtypes")
    static final Iterator none = new Iterator(){
        @Override public boolean hasNext() { return false; }
        @Override public Object next() { throw new NoSuchElementException(); }
    };

    @SuppressWarnings("rawtypes")
    static final Iterable empty = () -> none;

}
