package bsd.leveldb.io;

import java.util.Comparator;

public interface BinarySearch {

    interface Array<T> {
        T get(int i);
        int size();
    }

    static <T> Array<T> array(T[] array) {
        return new Array<T>(){
            @Override public T get(int i) { return array[i]; }
            @Override public int size() { return array.length; }
        };
    }

    // based on https://www.tbray.org/ongoing/When/200x/2003/03/22/Binary

    static <T> int floor(Array<T> list, T target, Comparator<T> comparator) {
        int high = list.size(), low = -1, mid, c;
        while (high - low > 1) {
            mid = (low + high) >>> 1;
            c = comparator.compare(list.get(mid),target);
            if (c > 0) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return low;
    }

    static <T> int ceiling(Array<T> list, T target, Comparator<T> comparator) {
        int high = list.size(), low = -1, mid, c;
        while (high - low > 1) {
            mid = (low + high) >>> 1;
            c = comparator.compare(list.get(mid),target);
            if (c < 0) {
                low = mid;
            } else {
                high = mid;
            }
        }
        return high < list.size() ? high : -1;
    }

    // similar to java.util.Arrays:binarySearch​(T[] a, T key, Comparator<? super T> c)

    static <T> int locate(Array<T> list, T target, Comparator<T> comparator) {
        int high = list.size(), low = -1, mid, c;
        while (high - low > 1) {
            mid = (low + high) >>> 1;
            c = comparator.compare(list.get(mid),target);
            if (c > 0) {
                high = mid;
            } else {
                if (c < 0) {
                    low = mid;
                } else {
                    return mid;
                }
            }
        }
        return -(low+1);
    }

}
