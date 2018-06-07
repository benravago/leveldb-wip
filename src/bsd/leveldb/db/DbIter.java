package bsd.leveldb.db;

import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.function.Consumer;

import bsd.leveldb.Cursor;
import bsd.leveldb.Slice;
import static bsd.leveldb.db.DbFormat.*;


/**
 * DBIter combines multiple entries for the same userkey
 * found in the DB representation into a single entry while
 * accounting for sequence numbers, deletion markers, overwrites, etc.
 *
 * Memtables and sstables that make the DB representation contain
 * (userkey,seq,type) => uservalue entries.
 */
class DbIter implements Cursor<Slice,Slice> {

    final Cursor<InternalKey,Slice> iter;
    final long sequence;
    final Consumer<InternalKey> recordReadSample;
    final Random rnd;
    long bytesCounter;

    DbIter(Cursor<InternalKey,Slice> iter, long sequence, Consumer<InternalKey> recordReadSample, int seed) {
        this.iter = iter;
        this.sequence = sequence;
        this.recordReadSample = recordReadSample;
        rnd = new Random(seed);
        bytesCounter = randomPeriod();
    }

    @Override
    public void close() {
        iter.close();
    }

    // Pick next gap with average value of config::kReadBytesPeriod.
    final int randomPeriod() {
        return rnd.nextInt( 2 * kReadBytesPeriod );
    }

    void parseKey(InternalKey k, Slice v) {
        int n = k.userKey.length + 8 + v.length;
        bytesCounter -= n;
        while (bytesCounter < 0) {
            bytesCounter += randomPeriod();
            recordReadSample.accept(k); // db_->RecordReadSample(k);
        }
    }

    boolean findNextUserKey() {
        // Loop until we hit an acceptable entry to yield
        while (iter.hasNext()) {
            iter.next();
            InternalKey k = iter.getKey();
            Slice v = iter.getValue();
            parseKey(k,v);
            if (k.userKey.equals(savedKey)) continue;
            // skip all succeeding entries for the same user key
            if (sequenceNumber(k) > sequence) continue;
            // skip all entries later than the required snapshot id
            savedKey = k.userKey;
            savedValue = (valueType(k) == kTypeValue) ? v : null;
            return true;
        }
        return false;
    }

    @Override public boolean hasNext() {
        if (!hasNext && iter.hasNext()) {
            hasNext = findNextUserKey();
        }
        return hasNext;
    }

    @Override
    public Entry<Slice,Slice> next() {
        if (hasNext()) {
            hasNext = false;
            return this;
        }
        throw new NoSuchElementException();
    }

    boolean hasNext;
    Slice savedKey;
    Slice savedValue;

    @Override public Slice getKey() { return savedKey; }
    @Override public Slice getValue() { return savedValue; }

}
