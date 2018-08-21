package bsd.leveldb.db;

import java.util.Comparator;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import bsd.leveldb.DB;
import bsd.leveldb.Options;
import bsd.leveldb.Slice;
import bsd.leveldb.WriteOptions;
import static bsd.leveldb.db.TestUtil.*;

public class AutocompactTest {

    String dbname;
    Options options = new Options();
    DB db;

    static Env env = new Env(){}; //   Cache* tiny_cache_;
    static Comparator<Slice> cmp = new BytewiseComparator();

    @BeforeClass
    public static void setUpClass() {
        tmpDir("./data");
    }

    @Before
    public void setUp() { // AutoCompactTest() {
        dbname = tmpDir() + "/autocompact_test"; // dbname_ = test::TmpDir() + "/autocompact_test";
        options.blockCacheSize = options.blockSize * 100; // tiny_cache_ = NewLRUCache(100);
        DB.destroyDB(options, dbname);
        options.createIfMissing = true;
        options.compression = Options.CompressionType.NoCompression;
        assertNotNull(db = DB.open(options, dbname));
    }

    @After
    public void tearDown() { // ~AutoCompactTest() {
        db.close(); //     delete db_;
        DB.destroyDB(options,dbname); // DestroyDB(dbname_, Options());
        // delete tiny_cache_;
    }

    String key(int i) {
        return String.format("key%06d",i);
    }

    long size(String start, String limit) {
        long size = db.getApproximateSize(s(start),s(limit));
        return size;
    }

    boolean put(WriteOptions options, String k, String v) {
        try { db.put(options,s(k),s(v)); return true; }
        catch (Throwable t) { return fault(t); }
    }

    boolean delete(WriteOptions options, String k) {
        try { db.delete(options,s(k)); return true; }
        catch (Throwable t) { return fault(t); }
    }

    boolean TEST_CompactMemTable(DbImpl dbi) {
        try { dbi.xCompactMemTable(); return true; }
        catch (Throwable t) { return fault(t); }
    }

    static final int kValueSize = 200 * 1024;
    static final int kTotalSize = 100 * 1024 * 1024;
    static final int kCount = kTotalSize / kValueSize;

    // Read through the first n keys repeatedly and check that they get
    // compacted (verified by checking the size of the key space).
    void doReads(int n) { // void AutoCompactTest::DoReads(int n) {
        String value = string(kValueSize, 'x');
        DbImpl dbi = (DbImpl) db.getProperty("leveldb.implementation"); // reinterpret_cast<DBImpl*>(db_);

        // Fill database
        for (int i = 0; i < kCount; i++) {
            // System.out.println(""+i+'/'+kCount);
            assertTrue(put(new WriteOptions(), key(i), value));
        }
        assertTrue(TEST_CompactMemTable(dbi));

        // Delete everything
        for (int i = 0; i < kCount; i++) {
            assertTrue(delete(new WriteOptions(), key(i)));
        }
        assertTrue(TEST_CompactMemTable(dbi));

        // Get initial measurement of the space we will be reading.
        long initialSize = size(key(0), key(n));
        long initialOtherSize = size(key(n), key(kCount));

        // Read until size drops significantly.
        Slice limitKey = s(key(n));
        for (int read = 0; true; read++) {
            assertTrue("Taking too long to compact", read < 100); // ASSERT_LT(read, 100) << "Taking too long to compact";
            for (Entry<Slice,Slice> iter : db) {
                if (cmp.compare(iter.getKey(),limitKey) >= 0) break;
                // Drop data
            }
            // Iterator* iter = db_->NewIterator(ReadOptions());
            // for (iter->SeekToFirst();
            //   iter->Valid() && iter->key().ToString() < limit_key;
            //   iter->Next()) {
            //   // Drop data
            // }
            // delete iter;

            // Wait a little bit to allow any triggered compactions to complete.
            env.sleepForMicroseconds(1000000); // Env::Default()->SleepForMicroseconds(1000000);
            long size = size(key(0), key(n));
            fprintf(stderr, "iter %3d => %7.3f MB [other %7.3f MB]\n",
                            read+1, size/1048576.0, size(key(n),key(kCount))/1048576.0 );

            if (size <= initialSize/10) {
                break;
            }
        }

        // Verify that the size of the key space not touched by the reads is pretty much unchanged.
        long finalOtherSize = size(key(n), key(kCount));
        assertTrue(finalOtherSize <= initialOtherSize + 1048576);
        assertTrue(finalOtherSize >= initialOtherSize/5 - 1048576);
    }

    @Test
    public void AutoCompactTest_ReadAll() {
        doReads(kCount);
    }

    @Test
    public void AutoCompactTest_ReadHalf() {
        doReads(kCount/2);
    }

}