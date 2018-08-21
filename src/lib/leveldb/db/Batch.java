package bsd.leveldb.db;

import java.util.Iterator;
import java.util.Map.Entry;

import bsd.leveldb.Slice;
import bsd.leveldb.WriteBatch;
import bsd.leveldb.Cursor;
import bsd.leveldb.io.ByteDecoder;
import bsd.leveldb.io.ByteEncoder;

class Batch {

    static abstract class Write implements Iterable<Entry<Slice,Slice>> {
        protected int count;
        protected long sequence;

        // Return the number of entries in the batch.
        int count() {
            return this.count;
        }
        // Return the sequence number for the start of this batch.
        long sequence() {
            return this.sequence;
        }
    }

    static class Item {
        Item next;
        Slice key;
        Slice value;
    }

    /**
     *  A modifiable WriteBatch implementation.
     */
    static class Que extends Write implements WriteBatch {
        Item head,tail;
        int approximateSize; // this assumes the average size of varint length prefix is 2

        // Set the count for the number of entries in the batch.
        void setCount(int count) {
            this.count = count;
        }
        // Store the specified number as the sequence number for the start of this batch.
        void setSequence(long sequence) {
            this.sequence = sequence;
        }

        int byteSize() {
            return kHeader + approximateSize;
        }
        Slice contents() {
            return encode(sequence,count,head);
        }

        void add(Slice key, Slice value) {
            Item item = new Item();
            item.key = key;
            item.value = value;

            if (head == null) {
                head = tail = item;
            } else {
                tail.next = item;
                tail = item;
            }
            count += 1;
        }

        @Override
        public void put(Slice key, Slice value) {
            add(key,value);
            approximateSize += ( 5 + key.length + value.length );
            // or ( 1 + width(key.length) + key.length + width(value.length) + value.length )
        }
        @Override
        public void delete(Slice key) {
            add(key,null);
            approximateSize += ( 3 + key.length );
            // or ( 1 + width(key.length) + key.length )
        }
        @Override
        public void clear() {
            head = tail = null;
            approximateSize = 0;
            count = 0;
        }

        void append(Que batch) {
            for (Entry<Slice,Slice> e : batch) {
                add(e.getKey(),e.getValue());
            }
        }

        @Override
        public Iterator<Entry<Slice, Slice>> iterator() {
            return cursor(head);
        }
    }

    static Cursor cursor(Item head) {
      return new Cursor() {
        Item que = head;
        Item next;
        @Override
        public boolean hasNext() {
            return que != null;
        }
        @Override
        public Entry<Slice,Slice> next() {
            next = que;
            que = que.next;
            return this;
        }
        @Override public Slice getKey() { return next.key; }
        @Override public Slice getValue() { return next.value; }
      };
    }

    // WriteBatch header is an 8-byte sequence number followed by a 4-byte count.
    static final int kHeader = 12;

    // WriteBatch::rep_ :=
    //    sequence: fixed64
    //    count: fixed32
    //    data: record[count]
    // record :=
    //    kTypeValue varstring varstring         |
    //    kTypeDeletion varstring
    // varstring :=
    //    len: varint32
    //    data: uint8[len]

    static Slice encode(long sequence, int count, Item items) {
        ByteEncoder buf = new ByteEncoder();
        buf.putFixed64(sequence);
        buf.putFixed32(count);
        for (Item i = items; i != null; i = i.next) {
            if (i.value != null) {
                buf.putFixed8(DbFormat.kTypeValue);
                buf.putLengthPrefixedSlice(i.key);
                buf.putLengthPrefixedSlice(i.value);
            } else {
                buf.putFixed8(DbFormat.kTypeDeletion);
                buf.putLengthPrefixedSlice(i.key);
            }
        }
        return buf.toSlice();
    }

    /**
     *  A partial, read-only WriteBatch implementation for decoding LOG files.
     */
    static class Rep extends Write {
        ByteDecoder buf = new ByteDecoder();

        void setContents(Slice record) {
            buf.wrap(record);
            sequence = buf.getFixed64();
            count = buf.getFixed32();
        }

        @Override
        public Iterator<Entry<Slice,Slice>> iterator() {
            return decode(buf,count);
        }
    }

    static Cursor decode(ByteDecoder buf, int count) {
      return new Cursor() {
        @Override
        public boolean hasNext() {
            if (buf.remaining() > 0) {
                return true;
            } else {
                assert (found == count);
                return false;
            }
        }
        @Override
        public Entry<Slice,Slice> next() {
            int type = buf.getByte();
            key = buf.getLengthPrefixedSlice();
            value = (type == DbFormat.kTypeValue)
                  ? buf.getLengthPrefixedSlice() : null;
            found += 1;
            return this;
        }
        int found = 0;
        Slice key, value;
        @Override public Slice getKey() { return key; }
        @Override public Slice getValue() { return value; }
      };
    }
}
