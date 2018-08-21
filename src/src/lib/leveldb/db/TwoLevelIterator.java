package lib.leveldb.db;

import java.util.Map.Entry;
import java.util.Iterator;
import java.util.function.Function;
import java.util.NoSuchElementException;

import lib.leveldb.Slice;
import lib.leveldb.Cursor;
import lib.leveldb.db.DbFormat.InternalKey;

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.

class TwoLevelIterator<V> implements Cursor<InternalKey,Slice> {

    Iterator<V> indexIter;
    Function<V,Cursor<InternalKey,Slice>> dataIter;

    TwoLevelIterator(
        Iterator<V> indexIter,
        Function<V,Cursor<InternalKey,Slice>> dataIter )
    {
        this.indexIter = indexIter;
        this.dataIter = dataIter;
    }

    @Override public boolean hasNext() {
        for (;;) {
            if (cursor == null) {
                if (indexIter.hasNext()) {
                    V arg = indexIter.next();
                    cursor = dataIter.apply(arg);
                } else {
                    return false;
                }
            }
            if (cursor != null) {
                if (cursor.hasNext()) {
                    return true;
                } else {
                    cursor = null;
                }
            }
        }
    }

    @Override public Entry<InternalKey, Slice> next() {
        if (hasNext()) {
            return cursor.next();
        }
        throw new NoSuchElementException();
    }

    Cursor<InternalKey,Slice> cursor;
    int sequence = 0;

    @Override public InternalKey getKey() { return cursor.getKey(); }
    @Override public Slice getValue() { return cursor.getValue(); }

    // TODO: handle close(); pass to current cursor
}
