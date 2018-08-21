package lib.leveldb.db;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.NoSuchElementException;

import lib.leveldb.Slice;
import lib.leveldb.Cursor;
import lib.leveldb.db.DbFormat.InternalKey;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0

class MergingIterator implements Cursor<InternalKey,Slice> {

    Comparator<InternalKey> icmp;
    Cursor<InternalKey,Slice>[] cursor;
    InternalKey[] key; // key cache
    int current = -1;

    MergingIterator(Comparator<InternalKey> comparator, Cursor<InternalKey,Slice>[] children) {
        icmp = comparator;
        cursor = children;
        key = new InternalKey[cursor.length];
        for (var i = 0; i < cursor.length; i++) {
            shift(i);
        }
    }

    int findSmallest() {
        var i = firstKey();
        if (i > -1) {
            i = smallestKey(i);
            if (i < key.length) {
                return i;
            }
        }
        return -1;
    }
    int firstKey() {
        for (var i = 0; i < key.length; i++) {
            if (key[i] != null) return i;
        }
        return -1;
    }
    int smallestKey(int f) {
        InternalKey s = key[f], k;
        for (var i = f+1; i < key.length; i++) {
            k = key[i];
            if (k != null && icmp.compare(k,s) < 0) {
                s = k; f = i;
            }
        }
        return f;
    }

    final void shift(int i) {
        var c = cursor[i];
        if (c.hasNext()) {
            key[i] = c.next().getKey();
        } else {
            key[i] = null;
            cursor[i] = null;
        }
    }

    @Override
    public boolean hasNext() {
        if (current < 0) {
            current = findSmallest();
        }
        return (current >= 0);
    }

    @Override
    public Entry<InternalKey,Slice> next() {
        if (hasNext()) {
            nextKey = key[current];
            nextValue = cursor[current].getValue();
            shift(current);
            current = -1;
            return this;
        }
        throw new NoSuchElementException();
    }

    InternalKey nextKey;
    Slice nextValue;

    @Override public InternalKey getKey() { return nextKey; }
    @Override public Slice getValue() { return nextValue; }

    static Cursor<InternalKey,Slice> of(KeyComparator<InternalKey> cmp, Cursor<InternalKey,Slice>[] list, int num) {
        if (num < list.length) {
            list = Arrays.copyOf(list,num);
        }
        switch (list.length) {
            case 0: return new Cursor(){}; // NewEmptyIterator();
            case 1: return list[0];
            default: return new MergingIterator(cmp,list);
        }
    }

}
