package bsd.leveldb.db;

import bsd.leveldb.Cursor;
import java.io.Closeable;

import java.util.Iterator;
import java.util.Map.Entry;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentSkipListMap;

import bsd.leveldb.Slice;
import static bsd.leveldb.db.DbFormat.*;

class MemTable implements Iterable<Entry<InternalKey,Slice>>, Closeable {

    InternalKeyComparator comparator;
    int refs;

    ConcurrentSkipListMap<InternalKey,Slice> table;
    AtomicLong approximateMemoryUsage;

    // MemTables are reference counted.
    // The initial reference count is zero and the caller must call Ref() at least once.

    MemTable(InternalKeyComparator cmp) {
        comparator = cmp;
        // refs_(0),
        table = new ConcurrentSkipListMap<>(this::orderKeysByLength);
        approximateMemoryUsage = new AtomicLong();
    }

    int orderKeysByLength(InternalKey a, InternalKey b) {
        int c = a.userKey.length - b.userKey.length;
        return c != 0 ? c : comparator.compare(a,b);
    }

    @Override
    public void close() { // MemTable::~MemTable()
        // assert(refs_ == 0);
    }

    /**
     * Increase reference count.
     */
    void ref() {
        ++refs;
    }

    /**
     * Drop reference count.  Delete if no more references exist.
     */
    void unref() {
        --refs;
        // assert(refs_ >= 0);
        // if (refs_ <= 0) {
        //   delete this;
        // }
    }

    /**
     * Returns an estimate of the number of bytes of data in use by this data structure.
     * It is safe to call when MemTable is being modified.
     */
    int approximateMemoryUsage() {
        return approximateMemoryUsage.intValue();
    }

    final static Slice nil = new Slice(null,0,0);

    /**
     * Add an entry into memtable that maps key to value at the specified sequence number and with the specified type.
     * Typically value will be empty if type==kTypeDeletion.
     */
    void add(long sequenceNumber, int valueType, Slice key, Slice value) {
        InternalKey internalKey = internalKey(key,sequenceNumber,valueType);
        if (valueType != kTypeValue) value = nil;
        table.put(internalKey,value);

        // entry format is:
        //    klength  varint32
        //    userkey  char[klength]
        //    tag      uint64
        //    vlength  varint32
        //    value    char[vlength]

        // assumes that average varint width of klength & vlength together is 5

        approximateMemoryUsage.addAndGet( 5 + key.length + kSequenceNumberSize + value.length );
    }

    /**
     * If memtable contains a value for key, store it in *value and return true.
     * If memtable contains a deletion for key, store a NotFound() error in *status and return true.
     * Else, return false.
     */
    Slice get(long sequenceNumber, Slice key) {
        InternalKey lookupKey = lookupKey(key,sequenceNumber);
        Entry<InternalKey,Slice> entry = table.ceilingEntry(lookupKey);
        if (entry != null) {
            InternalKey ik = entry.getKey();
            // Check that it belongs to same user key.  We do not check the
            // sequence number since the Seek() call above should have skipped
            // all entries with overly large sequence numbers.
            if (comparator.userComparator.compare(key,ik.userKey) == 0) {
                // Correct user key
                switch (valueType(ik)) {
                    case kTypeValue: return entry.getValue();
                    case kTypeDeletion: return null;
                }
            }
        }
        return null;
    }

    /**
     * Return an iterator that yields the contents of the memtable.
     *
     * The caller must ensure that the underlying MemTable remains live
     * while the returned iterator is live.  The keys returned by this
     * iterator are internal keys encoded by AppendInternalKey in the
     * db/format.{h,cc} module.
     */
    Cursor<InternalKey,Slice> newIterator() {
        return Struct.cursor(iterator());
    }

    @Override
    public Iterator<Entry<InternalKey,Slice>> iterator() {
        return table.entrySet().iterator();
    }
}