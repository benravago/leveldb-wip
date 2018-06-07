package bsd.leveldb.db;

import java.util.Iterator;

import bsd.leveldb.Slice;
import static bsd.leveldb.io.ByteDecoder.*;
import static bsd.leveldb.io.ByteEncoder.*;

interface DbFormat {

    // Grouping of constants.

    static final int kNumLevels = 7;

    // Level-0 compaction is started when we hit this many files.
    static final int kL0_CompactionTrigger = 4;

    // Soft limit on number of level-0 files.  We slow down writes at this point.
    static final int kL0_SlowdownWritesTrigger = 8;

    // Maximum number of level-0 files.  We stop writes at this point.
    static final int kL0_StopWritesTrigger = 12;

    // Maximum level to which a new compacted memtable is pushed if it
    // does not create overlap.  We try to push to level 2 to avoid the
    // relatively expensive level 0=>1 compactions and to avoid some
    // expensive manifest file operations.  We do not push all the way to
    // the largest level since that can generate a lot of wasted disk
    // space if the same key space is being repeatedly overwritten.
    static final int kMaxMemCompactLevel = 2;

    // Approximate gap in bytes between samples of data read during iteration.
    static final int kReadBytesPeriod = 1048576;

    // Value types encoded as the last component of internal keys.
    // DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk data structures.
    // enum ValueType {}
    static final int kTypeDeletion = 0x00;
    static final int kTypeValue = 0x01;

    // kValueTypeForSeek defines the ValueType that should be passed when
    // constructing a ParsedInternalKey object for seeking to a particular
    // sequence number (since we sort sequence numbers in decreasing order
    // and the value type is embedded as the low 8 bits in the sequence
    // number in internal keys, we need to use the highest-numbered
    // ValueType, not the lowest).
    static final int kValueTypeForSeek = kTypeValue;

    static final int kSequenceNumberSize = 8;

    // We leave eight bits empty at the bottom
    // so a type and sequence# can be packed together into 64-bits.
    static final long kMaxSequenceNumber = ((0x01L << 56) - 1);

    // size of a 32bit integer
    static final int sizeof_uint32_t = 4;

    // mask 32 bits from a long
    static final long mask_uint32_t = 0x0ffffffffL;

// struct ParsedInternalKey {
//   Slice user_key;
//   SequenceNumber sequence;
//   ValueType type;
//
//   ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
//   ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
//       : user_key(u), sequence(seq), type(t) { }
//   std::string DebugString() const;
// };

    // Return the length of the encoding of "key".
    static int internalKeyEncodingLength(InternalKey k) {
        return k.userKey.length + kSequenceNumberSize;
    }

    static Slice encodeInternalKey(InternalKey k) {
       return new Slice(appendInternalKey(k));
    }
    static InternalKey decodeInternalKey(Slice s) {
        assert (s.length >= kSequenceNumberSize);
        return parseInternalKey(s.data,s.offset,s.length);
    }

    static long packSequenceAndType(long seq, long t) {
        assert (seq <= kMaxSequenceNumber);
        assert (t <= kValueTypeForSeek);
        return (seq << 7) | t; // 1 sign bit + 56 sequence bits + 7 type bits
    }

    static long sequenceNumber(InternalKey key) {
        return ( key.sequence_type >>> 7 );
    }
    static int valueType(InternalKey key) {
        return (int)( key.sequence_type & 0x07fL );
    }

    /**
     * Append the serialization of "key" to *result.
     */
    static byte[] appendInternalKey(InternalKey key) {
        Slice src = key.userKey;
        byte[] dst = new byte[ src.length + kSequenceNumberSize ];
        System.arraycopy(src.data,src.offset,dst,0,src.length);
        encodeFixed64((key.sequence_type << 1),dst,src.length); // upper,56 sequence bits
        encodeFixed8(valueType(key),dst,src.length); // lower, 1 'sign' bit + 7 type bits
        return dst;
    }

    /**
     * Attempt to parse an internal key from "internal_key".
     * On success, stores the parsed data in "*result", and returns true.
     * On error, returns false, leaves "*result" in an undefined state.
     */
    static InternalKey parseInternalKey(byte[] b, int off, int len) {
        if (len < kSequenceNumberSize) return null;
        int keyLength = len - kSequenceNumberSize;
        Slice userKey = new Slice(b,off,keyLength);
        long num = decodeFixed64(b,off+keyLength);
        long sequence = num >>> 8;  // upper, 56 sequence bits
        int type = (int)(num & 0x0ffL);  // lower, 1 'sign' bit + 7 type bits
        return internalKey(userKey,sequence,type);
    }

    static boolean validInternalKey(InternalKey k) {
        return (k != null && k.userKey.length > 0 && valueType(k) <= kTypeValue);
    }

    static Iterable<Slice> extractUserKey(Iterator<InternalKey> i) {
        return () -> new Iterator<Slice>() {
            @Override public boolean hasNext() { return i.hasNext(); }
            @Override public Slice next() { return i.next().userKey; }
        };
    }

    /**
     * A comparator for internal keys that uses a specified comparator for
     * the user key portion and breaks ties by decreasing sequence number.
     */
    static class InternalKeyComparator implements KeyComparator<InternalKey> {

        final KeyComparator<Slice> userComparator;

        InternalKeyComparator(KeyComparator<Slice> userComparator) {
            this.userComparator = userComparator;
        }

        @Override
        public String name() { return "leveldb.InternalKeyComparator"; }

        @Override
        public int compare(InternalKey akey, InternalKey bkey) {
            if (akey == bkey) return 0;

            // Order by:
            //    increasing user key (according to user-supplied comparator)
            //    decreasing sequence number
            //    decreasing type (though sequence# should be enough to disambiguate)
            int r = userComparator.compare(akey.userKey,bkey.userKey);
            return r != 0 ? r
                 : akey.sequence_type > bkey.sequence_type ? -1
                 : akey.sequence_type < bkey.sequence_type ? 1
                 : 0;
        }

        @Override
        public InternalKey findShortestSeparator(InternalKey start, InternalKey limit) {
            // Attempt to shorten the user portion of the key
            Slice userStart = start.userKey;
            Slice userLimit = limit.userKey;
            Slice tmp = userComparator.findShortestSeparator(userStart,userLimit);
            if (tmp.length < userStart.length && userComparator.compare(userStart,tmp) < 0) {
                // User key has become shorter physically, but larger logically.
                // Tack on the earliest possible number to the shortened user key.
                InternalKey tmpKey = internalKey(tmp,kMaxSequenceNumber,kValueTypeForSeek);
                assert (compare(start, tmpKey) < 0);
                assert (compare(tmpKey, limit) < 0);
                start = tmpKey;
            }
            return start;
        }

        @Override
        public InternalKey findShortSuccessor(InternalKey key) {
            Slice userKey = key.userKey;
            Slice tmp = userComparator.findShortSuccessor(userKey);
            if (tmp.length < userKey.length && userComparator.compare(userKey,tmp) < 0) {
                // User key has become shorter physically, but larger logically.
                // Tack on the earliest possible number to the shortened user key.
                InternalKey tmpKey = internalKey(tmp,kMaxSequenceNumber,kValueTypeForSeek);
                assert (compare(key, tmpKey) < 0);
                key = tmpKey;
            }
            return key;
        }
    }

    /**
     * Modules in this directory should keep internal keys wrapped inside
     * the following class instead of plain strings so that we do not
     * incorrectly use string comparisons instead of an InternalKeyComparator.
     */
    static class InternalKey {
        InternalKey(Slice u, long s_t) {
            userKey = u; sequence_type = s_t;
        }
        final Slice userKey;
        final long sequence_type;
    }

    static InternalKey internalKey(Slice userKey, long sequenceNumber, int valueType) {
        return new InternalKey(userKey,packSequenceAndType(sequenceNumber,valueType));
    }

    static InternalKey lookupKey(Slice userKey, long sequenceNumber) {
        return internalKey(userKey,sequenceNumber,kValueTypeForSeek);
    }

}
