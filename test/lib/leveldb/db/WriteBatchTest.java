package bsd.leveldb.db;

import bsd.leveldb.DB;
import bsd.leveldb.Slice;
import bsd.leveldb.WriteBatch;
import static bsd.leveldb.db.DbFormat.*;

import java.util.Map.Entry;

import org.junit.Test;
import static org.junit.Assert.*;

public class WriteBatchTest {

    class WriteBatchInternal extends Batch.Rep implements WriteBatch {
        @Override public void put(Slice key, Slice value) {}
        @Override public void delete(Slice key) {}
        @Override public void clear() {}
    }

    static InternalKeyComparator cmp = new InternalKeyComparator(new BytewiseComparator());

    static String printContents(WriteBatch b) {
        MemTable mem = new MemTable(cmp);
        mem.ref();
        StringBuilder state = new StringBuilder();
        DbMain.insertInto((Batch.Write)b,mem);
        int count = 0;
        for (Entry<InternalKey,Slice> iter : mem) {
            InternalKey ikey = iter.getKey();
            // ParsedInternalKey ikey;
            // ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
            switch (valueType(ikey)) {
                case kTypeValue:
                    state.append("Put(");
                    state.append(toString(ikey.userKey));
                    state.append(", ");
                    state.append(toString(iter.getValue()));
                    state.append(")");
                    count++;
                    break;
                case kTypeDeletion:
                    state.append("Delete(");
                    state.append(toString(ikey.userKey));
                    state.append(")");
                    count++;
                    break;
            }
            state.append("@");
            state.append(sequenceNumber(ikey));
        }
        // delete iter;
        // if (!s.ok()) {
        //   state.append("ParseError()");
        if (count != count(b)) {
            state.append("CountMismatch()");
        }
        mem.unref();
        return state.toString();
    }

    static Slice slice(String s) {
        return new Slice(s.getBytes());
    }

    static String toString(Slice s) {
        return new String(s.data,s.offset,s.length);
    }

    static int count(WriteBatch batch) {
        return ((Batch.Que)batch).count();
    }

    static long sequence(WriteBatch batch) {
        return ((Batch.Que)batch).sequence();
    }

    static void setSequence(WriteBatch batch, long n) {
        ((Batch.Que)batch).setSequence(n);
    }

    static Slice contents(WriteBatch batch) {
        return ((Batch.Que)batch).contents();
    }

    static void append(WriteBatch b1, WriteBatch b2) {
        ((Batch.Que)b1).append(((Batch.Que)b2));
    }

    @Test
    public void WriteBatchTest_Empty() { // TEST(WriteBatchTest, Empty) {
        WriteBatch batch = DB.newBatch();
        assertEquals("", printContents(batch));
        assertEquals(0, count(batch));
    }

    @Test
    public void WriteBatchTest_Multiple() {
        WriteBatch batch = DB.newBatch();
        batch.put(slice("foo"), slice("bar"));
        batch.delete(slice("box"));
        batch.put(slice("baz"), slice("boo"));
        setSequence(batch, 100);
        assertEquals(100, sequence(batch));
        assertEquals(3, count(batch));
        assertEquals("Put(baz, boo)@102" +
                     "Delete(box)@101" +
                     "Put(foo, bar)@100",
                     printContents(batch));
    }

    @Test // (expected = AssertionError.class)
    public void WriteBatchTest_Corruption() {
        WriteBatch batch = DB.newBatch();
        batch.put(slice("foo"), slice("bar"));
        batch.delete(slice("box"));
        setSequence(batch, 200);
        Slice contents = contents(batch);

        WriteBatchInternal batch2 = new WriteBatchInternal();
        batch2.setContents(new Slice(contents.data,contents.offset,contents.length-1));
        try {
            assertEquals("Put(foo, bar)@200" +
                         "ParseError()",
                         printContents(batch2));
        }
        catch (Throwable t) {
            if (!assertionError(t,"ByteDecoder","getLengthPrefix")) {
                throw t;
            }
        }
    }

    static boolean assertionError(Throwable t, String ... n) {
        if (t instanceof AssertionError) {
            if (n.length < 1) return true;
            StackTraceElement e = t.getStackTrace()[0];
            if (e.getClassName().endsWith(n[0])) {
                return (n.length < 2) ? true
                     : e.getMethodName().equals(n[1]);
            }
        }
        return false;
    }

    @Test
    public void WriteBatchTest_Append() {
        WriteBatch b1 = DB.newBatch(), b2 = DB.newBatch();
        setSequence(b1, 200);
        setSequence(b2, 300);
        append(b1, b2);
        assertEquals("",
                     printContents(b1));
        b2.put(slice("a"),slice("va"));
        append(b1, b2);
        assertEquals("Put(a, va)@200",
                     printContents(b1));
        b2.clear();
        b2.put(slice("b"), slice("vb"));
        append(b1, b2);
        assertEquals("Put(a, va)@200" +
                     "Put(b, vb)@201",
                     printContents(b1));
        b2.delete(slice("foo"));
        append(b1, b2);
        assertEquals("Put(a, va)@200" +
                     "Put(b, vb)@202" +
                     "Put(b, vb)@201" +
                     "Delete(foo)@203",
                     printContents(b1));
    }

}
