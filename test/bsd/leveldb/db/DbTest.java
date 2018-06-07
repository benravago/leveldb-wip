package bsd.leveldb.db;

import bsd.leveldb.Cursor;
import java.nio.file.Path;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FilterOutputStream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

import bsd.leveldb.DB;
import bsd.leveldb.Slice;
import bsd.leveldb.Snapshot;
import bsd.leveldb.Options;
import bsd.leveldb.ReadOptions;
import bsd.leveldb.WriteOptions;
import bsd.leveldb.io.SeekableInputStream;
import static bsd.leveldb.Status.Code.*;
import static bsd.leveldb.db.LogFormat.*;
import static bsd.leveldb.db.TestUtil.*;

public class DbTest {
    public static void main(String[] a) throws Exception {
        DbTest.setUpClass();
        new DbTest().run();
        // DbTest.tearDownClass();
    }
    void run() throws Exception {
       setUp();
       // DBTest_GetFromImmutableLayer();
       tearDown();
    }

    static void say(String s) {System.out.println(s);}


    @Rule
    public TestName testName = new TestName();

    String testName() {
        String n = testName.getMethodName();
        return (n != null) ? n.replace("_",".") : n;
    }

// static std::string RandomString(Random* rnd, int len) {
//   std::string r;
//   test::RandomString(rnd, len, &r);
//   return r;
// }

    // Special Env used to delay background operations
    class SpecialEnv implements Env {

        // sstable/log Sync() calls are blocked while this pointer is non-NULL.
        AtomicBoolean delayDataSync;

        // sstable/log Sync() calls return an error.
        AtomicBoolean dataSyncError;

        // Simulate no-space errors while this pointer is non-NULL.
        AtomicBoolean noSpace;

        // Simulate non-writable file system while this pointer is non-NULL
        AtomicBoolean nonWritable;

        // Force sync of manifest files to fail while this pointer is non-NULL
        AtomicBoolean manifestSyncError;

        // Force write to manifest files to fail while this pointer is non-NULL
        AtomicBoolean manifestWriteError;

        boolean countRandomReads;
        AtomicLong randomReadCounter;

        Env target;

        SpecialEnv(Env target) {
            this.target = target;
            delayDataSync = new AtomicBoolean();
            dataSyncError = new AtomicBoolean();
            noSpace = new AtomicBoolean();
            nonWritable = new AtomicBoolean();
            randomReadCounter = new AtomicLong(); // count_random_reads_ = false;
            manifestSyncError = new AtomicBoolean();
            manifestWriteError = new AtomicBoolean();
        }

        void delayMilliseconds(int millis) {
            target.sleepForMicroseconds(millis * 1000);
        }

        @Override
        public void syncFile(OutputStream file) throws IOException {
            if (file instanceof SpecialFile) {
                ((SpecialFile)file).sync();
            } else {
                target.syncFile(file);
            }
        }

        @Override
        public OutputStream newWritableFile(Path fname) throws IOException { //   Status NewWritableFile(const std::string& f, WritableFile** r) {
            if (nonWritable.get()) {
                throw status(IOError,"simulated write error");
            }
            OutputStream s = target.newWritableFile(fname); //       Status Sync() {
            String f = fname.getFileName().toString(); // Status s = target()->NewWritableFile(f, r);
            // if (s.ok()) {
            if (f.endsWith(".ldb") || f.endsWith(".log")) { // if (strstr(f.c_str(), ".ldb") != NULL ||
                return newDataFile(s);                 //     strstr(f.c_str(), ".log") != NULL) {
            } else if (f.startsWith("MANIFEST-")) { // strstr(f.c_str(), "MANIFEST") != NULL) {
                return newManifestFile(s);
            }
            return s;
        }

        OutputStream newDataFile(OutputStream out) {
          return new SpecialFile(out) {
            @Override
            boolean append() {
                return ! noSpace.get();
                // Drop writes on the floor (if requested)
            }
            @Override
            void sync() throws IOException {
                if (dataSyncError.get()) {
                    throw status(IOError,"simulated data sync error");
                }
                while (delayDataSync.get()) {
                    delayMilliseconds(100);
                }
                target.syncFile(out);
            }
          };
        }

        OutputStream newManifestFile(OutputStream out) {
          return new SpecialFile(out) {
            @Override
            boolean append() {
                if (manifestWriteError.get()) {
                    throw status(IOError,"simulated writer error");
                }
                return true;
            }
            @Override
            void sync() throws IOException {
                if (manifestSyncError.get()) {
                    throw status(IOError,"simulated sync error");
                } else {
                    target.syncFile(out);
                }
            }
          };
        }

        class SpecialFile extends FilterOutputStream {
            SpecialFile(OutputStream out) {
                super(out);
            }
            boolean append() { return false; }
            void sync() throws IOException {}
            @Override
            public void write(int b) throws IOException {
                if (append()) super.write(b);
            }
            @Override
            public void write(byte[] b) throws IOException {
                if (append()) super.write(b);
            }
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                if (append()) super.write(b,off,len);
            }
        }

        @Override
        public SeekableInputStream newRandomAccessFile(Path fname) throws IOException {
          return new SeekableInputStream() {
            SeekableInputStream in = target.newRandomAccessFile(fname);

            @Override
            public int read() throws IOException {
                randomReadCounter.incrementAndGet(); return in.read();
            }
            @Override
            public int read(byte[] b) throws IOException {
                randomReadCounter.incrementAndGet(); return in.read(b);
            }
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                 randomReadCounter.incrementAndGet(); return in.read(b,off,len);
            }
            @Override public void close() throws IOException { in.close(); }
            @Override public void seek(long pos) throws IOException { in.seek(pos); }
            @Override public long length() throws IOException { return in.length(); }
          };
        }

    } // SpecialEnv

// class DBTest {
//  private:
    FilterPolicy filterPolicy;

    // Sequence of option configurations to try
    static final int // enum OptionConfig    void closeDB()
        kDefault = 0,
        kReuse = 1,
        kFilter = 2,
        kUncompressed = 3,
        kEnd = 4;

    int optionConfig;

//  public:
    String dbname;
    SpecialEnv env;
    DB db;

    Options lastOptions;

    @BeforeClass
    public static void setUpClass() {
        tmpDir("./data");
    }

    @Before
    public void setUp() { // DBTest() {
        System.out.println("=== Test "+testName());
        optionConfig = kDefault;
        env = new SpecialEnv(new Env(){});  // env_(new SpecialEnv(Env::Default())) {
        filterPolicy = new BloomFilterPolicy(10);
        dbname = tmpDir()+"/db_test";  // test::TmpDir() + "/db_test";
        destroyDB(dbname, new Options());
        db = null;
        reopen();
    }

    @After
    public void tearDown() { // ~DBTest() {
        close(); // delete db_;
        destroyDB(dbname, new Options());
        // delete env_;
        // delete filter_policy_;
    }

    // Switch to a fresh database with the next option configuration to test.
    // Return false if there are no more configurations to test.
    boolean changeOptions() {
        optionConfig++;
        if (optionConfig >= kEnd) {
            return false;
        } else {
            destroyAndReopen();
            return true;
        }
    }

    // Return the current option configuration.
    Options currentOptions() {
        Options options = new Options();
        options.reuseLogs = false;
        switch (optionConfig) {
            case kReuse:
                options.reuseLogs = true;
                break;
            case kFilter:
                options.filterPolicy = filterPolicy;
                break;
            case kUncompressed:
                options.compression = Options.CompressionType.NoCompression;
                break;
            default:
                break;
        }
        return options;
    }

    DbImpl dbfull() {
        return (DbImpl) db.getProperty("leveldb.implementation"); // reinterpret_cast<DBImpl*>(db_);
    }

    void TEST_CompactMemTable(DbImpl impl) { impl.xCompactMemTable(); }

    void reopen() {
        reopen(null);
    }
    void reopen(Options options) {
        assertTrue(tryReopen(options));
    }

//  DB open(Options options, String dbname) {
//      try { return DB.open(options,dbname); }
//      catch (Exception e) { e.printStackTrace(); }
//      return null;
//  }

    void close() {
        if (db != null) {
            db.close(); // delete db_;
            db = null;
        }
    }

    // https://stackoverflow.com/questions/473401/get-name-of-currently-executing-test-in-junit-4

    boolean destroyDB(String name, Options options) {
        // preserve(name,testName.getMethodName());
        DB.destroyDB(options, name);
        return true;
    }

    void destroyAndReopen() {
        destroyAndReopen(null);
    }
    void destroyAndReopen(Options options) {
        close(); // delete db_;
        destroyDB(dbname, new Options());
        assertTrue(tryReopen(options));
    }

    boolean tryReopen(Options options) {
        close(); // delete db_;
        Options opts;
        if (options != null) {
            opts = options;
        } else {
            opts = currentOptions();
            opts.createIfMissing = true;
        }
        lastOptions = opts;

        db = DB.open(opts, dbname);
        return (db != null);
    }

    boolean put(String k, String v) {
        return put(new WriteOptions(),k,v);
    }
    boolean put(WriteOptions options, String k, String v) {
        try { db.put(options,s(k),s(v)); return true; }
        catch (Throwable t) { return fault(t); }
    }

    boolean delete(String k) {
        return delete(new WriteOptions(),k);
    }
    boolean delete(WriteOptions options, String k) {
        try { db.delete(options,s(k)); return true; }
        catch (Throwable t) { return fault(t); }
    }

    String get(String k) {
        return get(k,null);
    }
    String get(String k, Snapshot snapshot) { //   std::string Get(const std::string& k, const Snapshot* snapshot = NULL) {
        try {
            ReadOptions options = new ReadOptions();
            options.snapshot = snapshot;
            String result;
            Slice s = db.get(options,s(k)); // Status s = db_->Get(options, k, &result);
            if (s == null) { // if (s.IsNotFound())
                result = "NOT_FOUND";
            } else { // if (!s.ok())
                result = s(s);
            }
            return result;
        }
        catch (Throwable t) { fault(t); return null; }
    }

//   // Return a string that contains all key,value pairs in order,
//   // formatted like "(k1->v1)(k2->v2)".
//   std::string Contents() {
//     std::vector<std::string> forward;
//     std::string result;
//     Iterator* iter = db_->NewIterator(ReadOptions());
//     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
//       std::string s = IterStatus(iter);
//       result.push_back('(');
//       result.append(s);
//       result.push_back(')');
//       forward.push_back(s);
//     }
//
//     // Check reverse iteration results are the reverse of forward results
//     size_t matched = 0;
//     for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
//       ASSERT_LT(matched, forward.size());
//       assertEquals(IterStatus(iter), forward[forward.size() - matched - 1]);
//       matched++;
//     }
//     assertEquals(matched, forward.size());
//
//     delete iter;
//     return result;
//   }

//   std::string AllEntriesFor(const Slice& user_key) {
//     Iterator* iter = dbfull()->TEST_NewInternalIterator();
//     InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
//     iter->Seek(target.Encode());
//     std::string result;
//     if (!iter->status().ok()) {
//       result = iter->status().ToString();
//     } else {
//       result = "[ ";
//       bool first = true;
//       while (iter->Valid()) {
//         ParsedInternalKey ikey;
//         if (!ParseInternalKey(iter->key(), &ikey)) {
//           result += "CORRUPTED";
//         } else {
//           if (last_options_.comparator->Compare(ikey.user_key, user_key) != 0) {
//             break;
//           }
//           if (!first) {
//             result += ", ";
//           }
//           first = false;
//           switch (ikey.type) {
//             case kTypeValue:
//               result += iter->value().ToString();
//               break;
//             case kTypeDeletion:
//               result += "DEL";
//               break;
//           }
//         }
//         iter->Next();
//       }
//       if (!first) {
//         result += " ";
//       }
//       result += "]";
//     }
//     delete iter;
//     return result;
//   }

//   int NumTableFilesAtLevel(int level) {
//     std::string property;
//     assertTrue(
//         db_->GetProperty("leveldb.num-files-at-level" + NumberToString(level),
//                          &property));
//     return atoi(property.c_str());
//   }

//   int TotalTableFiles() {
//     int result = 0;
//     for (int level = 0; level < config::kNumLevels; level++) {
//       result += NumTableFilesAtLevel(level);
//     }
//     return result;
//   }

//   // Return spread of files per level
//   std::string FilesPerLevel() {
//     std::string result;
//     int last_non_zero_offset = 0;
//     for (int level = 0; level < config::kNumLevels; level++) {
//       int f = NumTableFilesAtLevel(level);
//       char buf[100];
//       snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
//       result += buf;
//       if (f > 0) {
//         last_non_zero_offset = result.size();
//       }
//     }
//     result.resize(last_non_zero_offset);
//     return result;
//   }

//   int CountFiles() {
//     std::vector<std::string> files;
//     env_->GetChildren(dbname_, &files);
//     return static_cast<int>(files.size());
//   }

//   uint64_t Size(const Slice& start, const Slice& limit) {
//     Range r(start, limit);
//     uint64_t size;
//     db_->GetApproximateSizes(&r, 1, &size);
//     return size;
//   }

//   void Compact(const Slice& start, const Slice& limit) {
//     db_->CompactRange(&start, &limit);
//   }

//   // Do n memtable compactions, each of which produces an sstable
//   // covering the range [small,large].
//   void MakeTables(int n, const std::string& small, const std::string& large) {
//     for (int i = 0; i < n; i++) {
//       Put(small, "begin");
//       Put(large, "end");
//       TEST_CompactMemTable(dbfull());
//     }
//   }

//   // Prevent pushing of new sstables into deeper levels by adding
//   // tables that cover a specified range to all levels.
//   void FillLevels(const std::string& smallest, const std::string& largest) {
//     MakeTables(config::kNumLevels, smallest, largest);
//   }

//   void DumpFileCounts(const char* label) {
//     fprintf(stderr, "---\n%s:\n", label);
//     fprintf(stderr, "maxoverlap: %lld\n",
//             static_cast<long long>(
//                 dbfull()->TEST_MaxNextLevelOverlappingBytes()));
//     for (int level = 0; level < config::kNumLevels; level++) {
//       int num = NumTableFilesAtLevel(level);
//       if (num > 0) {
//         fprintf(stderr, "  level %3d : %d files\n", level, num);
//       }
//     }
//   }

//   std::string DumpSSTableList() {
//     std::string property;
//     db_->GetProperty("leveldb.sstables", &property);
//     return property;
//   }

    String iterStatus(Cursor<Slice,Slice> iter) {
        try {
            if (iter.hasNext()) {
                iter.next();
                return s(iter.getKey())+"->"+s(iter.getValue());
            }
        }
        catch (Exception ignore) {}
        return "(invalid)";
    }
//     std::string result;    
//     if (iter->Valid()) {
//       result = iter->key().ToString() + "->" + iter->value().ToString();
//     } else {
//       result = "(invalid)";
//     }
//     return result;
//   }

//   bool DeleteAnSSTFile() {
//     std::vector<std::string> filenames;
//     assertOK(env_->GetChildren(dbname_, &filenames));
//     uint64_t number;
//     FileType type;
//     for (size_t i = 0; i < filenames.size(); i++) {
//       if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
//         assertOK(env_->DeleteFile(TableFileName(dbname_, number)));
//         return true;
//       }
//     }
//     return false;
//   }

//   // Returns number of files renamed.
//   int RenameLDBToSST() {
//     std::vector<std::string> filenames;
//     assertOK(env_->GetChildren(dbname_, &filenames));
//     uint64_t number;
//     FileType type;
//     int files_renamed = 0;
//     for (size_t i = 0; i < filenames.size(); i++) {
//       if (ParseFileName(filenames[i], &number, &type) && type == kTableFile) {
//         const std::string from = TableFileName(dbname_, number);
//         const std::string to = SSTTableFileName(dbname_, number);
//         assertOK(env_->RenameFile(from, to));
//         files_renamed++;
//       }
//     }
//     return files_renamed;
//   }
// };

    @Test // 01
    public void DBTest_Empty() {
        do {
            assertNotNull(db);
            assertEquals("NOT_FOUND", get("foo"));
        } while (changeOptions());
    }

    @Test // 02
    public void DBTest_ReadWrite() {
        do {
            assertTrue(put("foo", "v1"));
            assertEquals("v1", get("foo"));
            assertTrue(put("bar", "v2"));
            assertTrue(put("foo", "v3"));
            assertEquals("v3", get("foo"));
            assertEquals("v2", get("bar"));
        } while (changeOptions());
    }

    @Test // 03
    public void DBTest_PutDeleteGet() {
        do {
            assertTrue(put(new WriteOptions(), "foo", "v1"));
            assertEquals("v1", get("foo"));
            assertTrue(put(new WriteOptions(), "foo", "v2"));
            assertEquals("v2", get("foo"));
            assertTrue(delete(new WriteOptions(), "foo"));
            assertEquals("NOT_FOUND", get("foo"));
        } while (changeOptions());
    }

    // @Test - 04 - goes into long wait
    public void DBTest_GetFromImmutableLayer() {
        do {
            Options options = currentOptions();
            options.env = env;
            options.writeBufferSize = 100000;  // Small write buffer
            reopen(options);

            assertTrue(put("foo", "v1"));
            assertEquals("v1", get("foo"));

            env.delayDataSync.set(true);                // Block sync calls
            put("k1", string(100000, 'x'));             // Fill memtable
            put("k2", string(100000, 'y'));             // Trigger compaction
            assertEquals("v1", get("foo"));
            env.delayDataSync.set(false);               // Release sync calls
        } while (changeOptions());
    }

    @Test // 05
    public void DBTest_GetFromVersions() {
        do {
            assertTrue(put("foo", "v1"));
            TEST_CompactMemTable(dbfull());
            assertEquals("v1", get("foo"));
        } while (changeOptions());
    }

    @Test // 06
    public void DBTest_GetMemUsage() {
        do {
            assertTrue(put("foo", "v1"));
            String val;
            assertNotNull(val = db.getProperty("leveldb.approximate-memory-usage"));
            int memUsage = Integer.parseInt(val);
            assertTrue(memUsage > 0);
            assertTrue(memUsage < 5*1024*1024);
        } while (changeOptions());
    }

    @Test // 07
    public void DBTest_GetSnapshot() {
        do {
            // Try with both a short key and a long key
            for (int i = 0; i < 2; i++) {
                String key = (i == 0) ? "foo" : string(200, 'x');
                assertTrue(put(key, "v1"));
                Snapshot s1 = db.getSnapshot();
                assertTrue(put(key, "v2"));
                assertEquals("v2", get(key));
                assertEquals("v1", get(key, s1));
                TEST_CompactMemTable(dbfull());
                assertEquals("v2", get(key));
                assertEquals("v1", get(key, s1));
                db.releaseSnapshot(s1);
            }
        } while (changeOptions());
    }

/*@Test 08*/  public void DBTest_GetLevel0Ordering() {
        do {
//     // Check that we process level-0 files in correct order.  The code
//     // below generates two level-0 files where the earlier one comes
//     // before the later one in the level-0 file list since the earlier
//     // one has a smaller "smallest" key.
//     assertTrue(put("bar", "b"));
//     assertTrue(put("foo", "v1"));
//     TEST_CompactMemTable(dbfull());
//     assertTrue(put("foo", "v2"));
//     TEST_CompactMemTable(dbfull());
//     assertEquals("v2", get("foo"));
        } while (changeOptions());
    }

/*@Test 09*/  public void DBTest_GetOrderedByLevels() {
        do {
//     assertTrue(put("foo", "v1"));
//     Compact("a", "z");
//     assertEquals("v1", get("foo"));
//     assertTrue(put("foo", "v2"));
//     assertEquals("v2", get("foo"));
//     TEST_CompactMemTable(dbfull());
//     assertEquals("v2", get("foo"));
        } while (changeOptions());
    }

/*@Test 10*/  public void DBTest_GetPicksCorrectFile() {
        do {
//     // Arrange to have multiple files in a non-level-0 level.
//     assertTrue(put("a", "va"));
//     Compact("a", "b");
//     assertTrue(put("x", "vx"));
//     Compact("x", "y");
//     assertTrue(put("f", "vf"));
//     Compact("f", "g");
//     assertEquals("va", get("a"));
//     assertEquals("vf", get("f"));
//     assertEquals("vx", get("x"));
        } while (changeOptions());
    }

/*@Test 11*/  public void DBTest_GetEncountersEmptyLevel() {
        do {
//     // Arrange for the following to happen:
//     //   * sstable A in level 0
//     //   * nothing in level 1
//     //   * sstable B in level 2
//     // Then do enough Get() calls to arrange for an automatic compaction
//     // of sstable A.  A bug would cause the compaction to be marked as
//     // occurring at level 1 (instead of the correct level 0).
//
//     // Step 1: First place sstables in levels 0 and 2
//     int compaction_count = 0;
//     while (NumTableFilesAtLevel(0) == 0 ||
//            NumTableFilesAtLevel(2) == 0) {
//       ASSERT_LE(compaction_count, 100) << "could not fill levels 0 and 2";
//       compaction_count++;
//       Put("a", "begin");
//       Put("z", "end");
//       TEST_CompactMemTable(dbfull());
//     }
//
//     // Step 2: clear level 1 if necessary.
//     dbfull()->TEST_CompactRange(1, NULL, NULL);
//     assertEquals(NumTableFilesAtLevel(0), 1);
//     assertEquals(NumTableFilesAtLevel(1), 0);
//     assertEquals(NumTableFilesAtLevel(2), 1);
//
//     // Step 3: read a bunch of times
//     for (int i = 0; i < 1000; i++) {
//       assertEquals("NOT_FOUND", get("missing"));
//     }
//
//     // Step 4: Wait for compaction to finish
//     DelayMilliseconds(1000);
//
//     assertEquals(NumTableFilesAtLevel(0), 0);
        } while (changeOptions());
    }

    @Test // 12
    public void DBTest_IterEmpty() {
        Cursor<Slice,Slice> iter = db.iterator(new ReadOptions());
        assertEquals(iterStatus(iter), "(invalid)");
    }
//   iter->SeekToFirst();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToLast();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->Seek("foo");
//   assertEquals(IterStatus(iter), "(invalid)");
//   delete iter;

    @Test // 13
    public void DBTest_IterSingle() {
        assertTrue(put("a", "va"));
        Cursor<Slice,Slice> iter = db.iterator(new ReadOptions());
        assertEquals(iterStatus(iter), "a->va");
        assertEquals(iterStatus(iter), "(invalid)");
    }
//   iter->SeekToFirst();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToFirst();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToLast();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToLast();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->Seek("");
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->Seek("a");
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->Seek("b");
//   assertEquals(IterStatus(iter), "(invalid)");
//   delete iter;

    @Test // 14
    public void DBTest_IterMulti() {
        assertTrue(put("a", "va"));
        assertTrue(put("b", "vb"));
        assertTrue(put("c", "vc"));
        Cursor<Slice,Slice> iter = db.iterator(new ReadOptions());
        assertEquals(iterStatus(iter), "a->va");
        assertEquals(iterStatus(iter), "b->vb");
        assertEquals(iterStatus(iter), "c->vc");
        assertEquals(iterStatus(iter), "(invalid)");
    }    
//   iter->SeekToFirst();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "b->vb");
//   iter->Next();
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToFirst();//
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToLast();//
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "b->vb");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToLast();
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->Seek("");
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Seek("a");
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Seek("ax");
//   assertEquals(IterStatus(iter), "b->vb");
//   iter->Seek("b");
//   assertEquals(IterStatus(iter), "b->vb");
//   iter->Seek("z");
//   assertEquals(IterStatus(iter), "(invalid)");
//   // Switch from reverse to forward
//   iter->SeekToLast();
//   iter->Prev();
//   iter->Prev();
//   iter->Next();
//   assertEquals(IterStatus(iter), "b->vb");
//   // Switch from forward to reverse
//   iter->SeekToFirst();
//   iter->Next();
//   iter->Next();
//   iter->Prev();
//   assertEquals(IterStatus(iter), "b->vb");
//   // Make sure iter stays at snapshot
//   assertTrue(put("a",  "va2"));
//   assertTrue(put("a2", "va3"));
//   assertTrue(put("b",  "vb2"));
//   assertTrue(put("c",  "vc2"));
//   assertOK(Delete("b"));
//   iter->SeekToFirst();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "b->vb");//
//   iter->Next();
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//   iter->SeekToLast();
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "b->vb");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "(invalid)");
//   delete iter;

/*@Test 15*/  public void DBTest_IterSmallAndLargeMix() {
//   assertTrue(put("a", "va"));
//   assertTrue(put("b", std::string(100000, 'b')));
//   assertTrue(put("c", "vc"));
//   assertTrue(put("d", std::string(100000, 'd')));
//   assertTrue(put("e", std::string(100000, 'e')));
//
//   Iterator* iter = db_->NewIterator(ReadOptions());
//
//   iter->SeekToFirst();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Next();
//   assertEquals(IterStatus(iter), "b->" + std::string(100000, 'b'));
//   iter->Next();
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Next();
//   assertEquals(IterStatus(iter), "d->" + std::string(100000, 'd'));
//   iter->Next();
//   assertEquals(IterStatus(iter), "e->" + std::string(100000, 'e'));
//   iter->Next();
//   assertEquals(IterStatus(iter), "(invalid)");
//
//   iter->SeekToLast();
//   assertEquals(IterStatus(iter), "e->" + std::string(100000, 'e'));
//   iter->Prev();
//   assertEquals(IterStatus(iter), "d->" + std::string(100000, 'd'));
//   iter->Prev();
//   assertEquals(IterStatus(iter), "c->vc");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "b->" + std::string(100000, 'b'));
//   iter->Prev();
//   assertEquals(IterStatus(iter), "a->va");
//   iter->Prev();
//   assertEquals(IterStatus(iter), "(invalid)");
//
//   delete iter;
    }

/*@Test 16*/  public void DBTest_IterMultiWithDelete() {
        do {
//     assertTrue(put("a", "va"));
//     assertTrue(put("b", "vb"));
//     assertTrue(put("c", "vc"));
//     assertOK(Delete("b"));
//     assertEquals("NOT_FOUND", get("b"));
//
//     Iterator* iter = db_->NewIterator(ReadOptions());
//     iter->Seek("c");
//     assertEquals(IterStatus(iter), "c->vc");
//     iter->Prev();
//     assertEquals(IterStatus(iter), "a->va");
//     delete iter;
        } while (changeOptions());
    }

/*@Test 17*/  public void DBTest_Recover() {
        do {
//     assertTrue(put("foo", "v1"));
//     assertTrue(put("baz", "v5"));
//
//     Reopen();
//     assertEquals("v1", get("foo"));
//
//     assertEquals("v1", get("foo"));
//     assertEquals("v5", get("baz"));
//     assertTrue(put("bar", "v2"));
//     assertTrue(put("foo", "v3"));
//
//     Reopen();
//     assertEquals("v3", get("foo"));
//     assertTrue(put("foo", "v4"));
//     assertEquals("v4", get("foo"));
//     assertEquals("v2", get("bar"));
//     assertEquals("v5", get("baz"));
        } while (changeOptions());
    }

/*@Test 18*/  public void DBTest_RecoveryWithEmptyLog() {
        do {
//     assertTrue(put("foo", "v1"));
//     assertTrue(put("foo", "v2"));
//     Reopen();
//     Reopen();
//     assertTrue(put("foo", "v3"));
//     Reopen();
//     assertEquals("v3", get("foo"));
        } while (changeOptions());
    }

    // Check that writes done during a memtable compaction are recovered
    // if the database is shutdown during the memtable compaction.
/*@Test 19*/  public void DBTest_RecoverDuringMemtableCompaction() {
        do {
//     Options options = CurrentOptions();
//     options.env = env_;
//     options.write_buffer_size = 1000000;
//     Reopen(&options);
//
//     // Trigger a long memtable compaction and reopen the database during it
//     assertTrue(put("foo", "v1"));                         // Goes to 1st log file
//     assertTrue(put("big1", std::string(10000000, 'x')));  // Fills memtable
//     assertTrue(put("big2", std::string(1000, 'y')));      // Triggers compaction
//     assertTrue(put("bar", "v2"));                         // Goes to new log file
//
//     Reopen(&options);
//     assertEquals("v1", get("foo"));
//     assertEquals("v2", get("bar"));
//     assertEquals(std::string(10000000, 'x'), get("big1"));
//     assertEquals(std::string(1000, 'y'), get("big2"));
        } while (changeOptions());
    }

    static String key(int i) {
        return String.format("key%06d", i);
    }

/*@Test 20*/  public void DBTest_MinorCompactionsHappen() {
//   Options options = CurrentOptions();
//   options.write_buffer_size = 10000;
//   Reopen(&options);
//
//   const int N = 500;
//
//   int starting_num_tables = TotalTableFiles();
//   for (int i = 0; i < N; i++) {
//     assertTrue(put(Key(i), Key(i) + std::string(1000, 'v')));
//   }
//   int ending_num_tables = TotalTableFiles();
//   ASSERT_GT(ending_num_tables, starting_num_tables);
//
//   for (int i = 0; i < N; i++) {
//     assertEquals(Key(i) + std::string(1000, 'v'), get(Key(i)));
//   }
//
//   Reopen();
//
//   for (int i = 0; i < N; i++) {
//     assertEquals(Key(i) + std::string(1000, 'v'), get(Key(i)));
//   }
    }

/*@Test 21*/  public void DBTest_RecoverWithLargeLog() {
//   {
//     Options options = CurrentOptions();
//     Reopen(&options);
//     assertTrue(put("big1", std::string(200000, '1')));
//     assertTrue(put("big2", std::string(200000, '2')));
//     assertTrue(put("small3", std::string(10, '3')));
//     assertTrue(put("small4", std::string(10, '4')));
//     assertEquals(NumTableFilesAtLevel(0), 0);
//   }
//
//   // Make sure that if we re-open with a small write buffer size that
//   // we flush table files in the middle of a large log file.
//   Options options = CurrentOptions();
//   options.write_buffer_size = 100000;
//   Reopen(&options);
//   assertEquals(NumTableFilesAtLevel(0), 3);
//   assertEquals(std::string(200000, '1'), get("big1"));
//   assertEquals(std::string(200000, '2'), get("big2"));
//   assertEquals(std::string(10, '3'), get("small3"));
//   assertEquals(std::string(10, '4'), get("small4"));
//   ASSERT_GT(NumTableFilesAtLevel(0), 1);
    }

/*@Test 22*/  public void DBTest_CompactionsGenerateMultipleFiles() {
//   Options options = CurrentOptions();
//   options.write_buffer_size = 100000000;        // Large write buffer
//   Reopen(&options);
//
//   Random rnd(301);
//
//   // Write 8MB (80 values, each 100K)
//   assertEquals(NumTableFilesAtLevel(0), 0);
//   std::vector<std::string> values;
//   for (int i = 0; i < 80; i++) {
//     values.push_back(RandomString(&rnd, 100000));
//     assertTrue(put(Key(i), values[i]));
//   }
//
//   // Reopening moves updates to level-0
//   Reopen(&options);
//   dbfull()->TEST_CompactRange(0, NULL, NULL);
//
//   assertEquals(NumTableFilesAtLevel(0), 0);
//   ASSERT_GT(NumTableFilesAtLevel(1), 1);
//   for (int i = 0; i < 80; i++) {
//     assertEquals(Get(Key(i)), values[i]);
//   }
    }

/*@Test 23*/  public void DBTest_RepeatedWritesToSameKey() {
//   Options options = CurrentOptions();
//   options.env = env_;
//   options.write_buffer_size = 100000;  // Small write buffer
//   Reopen(&options);
//
//   // We must have at most one file per level except for level-0,
//   // which may have up to kL0_StopWritesTrigger files.
//   const int kMaxFiles = config::kNumLevels + config::kL0_StopWritesTrigger;
//
//   Random rnd(301);
//   std::string value = RandomString(&rnd, 2 * options.write_buffer_size);
//   for (int i = 0; i < 5 * kMaxFiles; i++) {
//     Put("key", value);
//     ASSERT_LE(TotalTableFiles(), kMaxFiles);
//     fprintf(stderr, "after %d: %d files\n", int(i+1), TotalTableFiles());
//   }
    }

/*@Test 24*/  public void DBTest_SparseMerge() {
//   Options options = CurrentOptions();
//   options.compression = kNoCompression;
//   Reopen(&options);
//
//   FillLevels("A", "Z");
//
//   // Suppose there is:
//   //    small amount of data with prefix A
//   //    large amount of data with prefix B
//   //    small amount of data with prefix C
//   // and that recent updates have made small changes to all three prefixes.
//   // Check that we do not do a compaction that merges all of B in one shot.
//   const std::string value(1000, 'x');
//   Put("A", "va");
//   // Write approximately 100MB of "B" values
//   for (int i = 0; i < 100000; i++) {
//     char key[100];
//     snprintf(key, sizeof(key), "B%010d", i);
//     Put(key, value);
//   }
//   Put("C", "vc");
//   TEST_CompactMemTable(dbfull());
//   dbfull()->TEST_CompactRange(0, NULL, NULL);
//
//   // Make sparse update
//   Put("A",    "va2");
//   Put("B100", "bvalue2");
//   Put("C",    "vc2");
//   TEST_CompactMemTable(dbfull());
//
//   // Compactions should not cause us to create a situation where
//   // a file overlaps too much data at the next level.
//   ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
//   dbfull()->TEST_CompactRange(0, NULL, NULL);
//   ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
//   dbfull()->TEST_CompactRange(1, NULL, NULL);
//   ASSERT_LE(dbfull()->TEST_MaxNextLevelOverlappingBytes(), 20*1048576);
    }

// static bool Between(uint64_t val, uint64_t low, uint64_t high) {
//   bool result = (val >= low) && (val <= high);
//   if (!result) {
//     fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
//             (unsigned long long)(val),
//             (unsigned long long)(low),
//             (unsigned long long)(high));
//   }
//   return result;
// }

/*@Test 25*/  public void DBTest_ApproximateSizes() {
        do {
//     Options options = CurrentOptions();
//     options.write_buffer_size = 100000000;        // Large write buffer
//     options.compression = kNoCompression;
//     DestroyAndReopen();
//
//     assertTrue(Between(Size("", "xyz"), 0, 0));
//     Reopen(&options);
//     assertTrue(Between(Size("", "xyz"), 0, 0));
//
//     // Write 8MB (80 values, each 100K)
//     assertEquals(NumTableFilesAtLevel(0), 0);
//     const int N = 80;
//     static const int S1 = 100000;
//     static const int S2 = 105000;  // Allow some expansion from metadata
//     Random rnd(301);
//     for (int i = 0; i < N; i++) {
//       assertTrue(put(Key(i), RandomString(&rnd, S1)));
//     }
//
//     // 0 because GetApproximateSizes() does not account for memtable space
//     assertTrue(Between(Size("", Key(50)), 0, 0));
//
//     if (options.reuse_logs) {
//       // Recovery will reuse memtable, and GetApproximateSizes() does not
//       // account for memtable usage;
//       Reopen(&options);
//       assertTrue(Between(Size("", Key(50)), 0, 0));
//       continue;
//     }
//
//     // Check sizes across recovery by reopening a few times
//     for (int run = 0; run < 3; run++) {
//       Reopen(&options);
//
//       for (int compact_start = 0; compact_start < N; compact_start += 10) {
//         for (int i = 0; i < N; i += 10) {
//           assertTrue(Between(Size("", Key(i)), S1*i, S2*i));
//           assertTrue(Between(Size("", Key(i)+".suffix"), S1*(i+1), S2*(i+1)));
//           assertTrue(Between(Size(Key(i), Key(i+10)), S1*10, S2*10));
//         }
//         assertTrue(Between(Size("", Key(50)), S1*50, S2*50));
//         assertTrue(Between(Size("", Key(50)+".suffix"), S1*50, S2*50));
//
//         std::string cstart_str = Key(compact_start);
//         std::string cend_str = Key(compact_start + 9);
//         Slice cstart = cstart_str;
//         Slice cend = cend_str;
//         dbfull()->TEST_CompactRange(0, &cstart, &cend);
//       }
//
//       assertEquals(NumTableFilesAtLevel(0), 0);
//       ASSERT_GT(NumTableFilesAtLevel(1), 0);
//     }
        } while (changeOptions());
    }

/*@Test 26*/  public void DBTest_ApproximateSizes_MixOfSmallAndLarge() {
        do {
//     Options options = CurrentOptions();
//     options.compression = kNoCompression;
//     Reopen();
//
//     Random rnd(301);
//     std::string big1 = RandomString(&rnd, 100000);
//     assertTrue(put(Key(0), RandomString(&rnd, 10000)));
//     assertTrue(put(Key(1), RandomString(&rnd, 10000)));
//     assertTrue(put(Key(2), big1));
//     assertTrue(put(Key(3), RandomString(&rnd, 10000)));
//     assertTrue(put(Key(4), big1));
//     assertTrue(put(Key(5), RandomString(&rnd, 10000)));
//     assertTrue(put(Key(6), RandomString(&rnd, 300000)));
//     assertTrue(put(Key(7), RandomString(&rnd, 10000)));
//
//     if (options.reuse_logs) {
//       // Need to force a memtable compaction since recovery does not do so.
//       assertOK(TEST_CompactMemTable(dbfull()));
//     }
//
//     // Check sizes across recovery by reopening a few times
//     for (int run = 0; run < 3; run++) {
//       Reopen(&options);
//
//       assertTrue(Between(Size("", Key(0)), 0, 0));
//       assertTrue(Between(Size("", Key(1)), 10000, 11000));
//       assertTrue(Between(Size("", Key(2)), 20000, 21000));
//       assertTrue(Between(Size("", Key(3)), 120000, 121000));
//       assertTrue(Between(Size("", Key(4)), 130000, 131000));
//       assertTrue(Between(Size("", Key(5)), 230000, 231000));
//       assertTrue(Between(Size("", Key(6)), 240000, 241000));
//       assertTrue(Between(Size("", Key(7)), 540000, 541000));
//       assertTrue(Between(Size("", Key(8)), 550000, 560000));
//
//       assertTrue(Between(Size(Key(3), Key(5)), 110000, 111000));
//
//       dbfull()->TEST_CompactRange(0, NULL, NULL);
//     }
        } while (changeOptions());
    }

/*@Test 27*/  public void DBTest_IteratorPinsRef() {
//   Put("foo", "hello");
//
//   // Get iterator that will yield the current contents of the DB.
//   Iterator* iter = db_->NewIterator(ReadOptions());
//
//   // Write to force compactions
//   Put("foo", "newvalue1");
//   for (int i = 0; i < 100; i++) {
//     assertTrue(put(Key(i), Key(i) + std::string(100000, 'v'))); // 100K values
//   }
//   Put("foo", "newvalue2");
//
//   iter->SeekToFirst();
//   assertTrue(iter->Valid());
//   assertEquals("foo", iter->key().ToString());
//   assertEquals("hello", iter->value().ToString());
//   iter->Next();
//   assertTrue(!iter->Valid());
//   delete iter;
    }

/*@Test 28*/  public void DBTest_Snapshot() {
        do {
//     Put("foo", "v1");
//     const Snapshot* s1 = db_->GetSnapshot();
//     Put("foo", "v2");
//     const Snapshot* s2 = db_->GetSnapshot();
//     Put("foo", "v3");
//     const Snapshot* s3 = db_->GetSnapshot();
//
//     Put("foo", "v4");
//     assertEquals("v1", get("foo", s1));
//     assertEquals("v2", get("foo", s2));
//     assertEquals("v3", get("foo", s3));
//     assertEquals("v4", get("foo"));
//
//     db_->ReleaseSnapshot(s3);
//     assertEquals("v1", get("foo", s1));
//     assertEquals("v2", get("foo", s2));
//     assertEquals("v4", get("foo"));
//
//     db_->ReleaseSnapshot(s1);
//     assertEquals("v2", get("foo", s2));
//     assertEquals("v4", get("foo"));
//
//     db_->ReleaseSnapshot(s2);
//     assertEquals("v4", get("foo"));
        } while (changeOptions());
    }

/*@Test 29*/  public void DBTest_HiddenValuesAreRemoved() {
        do {
//     Random rnd(301);
//     FillLevels("a", "z");
//
//     std::string big = RandomString(&rnd, 50000);
//     Put("foo", big);
//     Put("pastfoo", "v");
//     const Snapshot* snapshot = db_->GetSnapshot();
//     Put("foo", "tiny");
//     Put("pastfoo2", "v2");        // Advance sequence number one more
//
//     assertOK(TEST_CompactMemTable(dbfull()));
//     ASSERT_GT(NumTableFilesAtLevel(0), 0);
//
//     assertEquals(big, get("foo", snapshot));
//     assertTrue(Between(Size("", "pastfoo"), 50000, 60000));
//     db_->ReleaseSnapshot(snapshot);
//     assertEquals(AllEntriesFor("foo"), "[ tiny, " + big + " ]");
//     Slice x("x");
//     dbfull()->TEST_CompactRange(0, NULL, &x);
//     assertEquals(AllEntriesFor("foo"), "[ tiny ]");
//     assertEquals(NumTableFilesAtLevel(0), 0);
//     ASSERT_GE(NumTableFilesAtLevel(1), 1);
//     dbfull()->TEST_CompactRange(1, NULL, &x);
//     assertEquals(AllEntriesFor("foo"), "[ tiny ]");
//
//     assertTrue(Between(Size("", "pastfoo"), 0, 1000));
        } while (changeOptions());
    }

/*@Test 30*/  public void DBTest_DeletionMarkers1() {
//   Put("foo", "v1");
//   assertOK(TEST_CompactMemTable(dbfull()));
//   const int last = config::kMaxMemCompactLevel;
//   assertEquals(NumTableFilesAtLevel(last), 1);   // foo => v1 is now in last level
//
//   // Place a table at level last-1 to prevent merging with preceding mutation
//   Put("a", "begin");
//   Put("z", "end");
//   TEST_CompactMemTable(dbfull());
//   assertEquals(NumTableFilesAtLevel(last), 1);
//   assertEquals(NumTableFilesAtLevel(last-1), 1);
//
//   Delete("foo");
//   Put("foo", "v2");
//   assertEquals(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
//   assertOK(TEST_CompactMemTable(dbfull()));  // Moves to level last-2
//   assertEquals(AllEntriesFor("foo"), "[ v2, DEL, v1 ]");
//   Slice z("z");
//   dbfull()->TEST_CompactRange(last-2, NULL, &z);
//   // DEL eliminated, but v1 remains because we aren't compacting that level
//   // (DEL can be eliminated because v2 hides v1).
//   assertEquals(AllEntriesFor("foo"), "[ v2, v1 ]");
//   dbfull()->TEST_CompactRange(last-1, NULL, NULL);
//   // Merging last-1 w/ last, so we are the base level for "foo", so
//   // DEL is removed.  (as is v1).
//   assertEquals(AllEntriesFor("foo"), "[ v2 ]");
    }

/*@Test 31*/  public void DBTest_DeletionMarkers2() {
//   Put("foo", "v1");
//   assertOK(TEST_CompactMemTable(dbfull()));
//   const int last = config::kMaxMemCompactLevel;
//   assertEquals(NumTableFilesAtLevel(last), 1);   // foo => v1 is now in last level
//
//   // Place a table at level last-1 to prevent merging with preceding mutation
//   Put("a", "begin");
//   Put("z", "end");
//   TEST_CompactMemTable(dbfull());
//   assertEquals(NumTableFilesAtLevel(last), 1);
//   assertEquals(NumTableFilesAtLevel(last-1), 1);
//
//   Delete("foo");
//   assertEquals(AllEntriesFor("foo"), "[ DEL, v1 ]");
//   assertOK(TEST_CompactMemTable(dbfull()));  // Moves to level last-2
//   assertEquals(AllEntriesFor("foo"), "[ DEL, v1 ]");
//   dbfull()->TEST_CompactRange(last-2, NULL, NULL);
//   // DEL kept: "last" file overlaps
//   assertEquals(AllEntriesFor("foo"), "[ DEL, v1 ]");
//   dbfull()->TEST_CompactRange(last-1, NULL, NULL);
//   // Merging last-1 w/ last, so we are the base level for "foo", so
//   // DEL is removed.  (as is v1).
//   assertEquals(AllEntriesFor("foo"), "[ ]");
    }

/*@Test 32*/  public void DBTest_OverlapInLevel0() {
        do {
//     assertEquals(config::kMaxMemCompactLevel, 2) << "Fix test to match config";
//
//     // Fill levels 1 and 2 to disable the pushing of new memtables to levels > 0.
//     assertTrue(put("100", "v100"));
//     assertTrue(put("999", "v999"));
//     TEST_CompactMemTable(dbfull());
//     assertOK(Delete("100"));
//     assertOK(Delete("999"));
//     TEST_CompactMemTable(dbfull());
//     assertEquals("0,1,1", FilesPerLevel());
//
//     // Make files spanning the following ranges in level-0:
//     //  files[0]  200 .. 900
//     //  files[1]  300 .. 500
//     // Note that files are sorted by smallest key.
//     assertTrue(put("300", "v300"));
//     assertTrue(put("500", "v500"));
//     TEST_CompactMemTable(dbfull());
//     assertTrue(put("200", "v200"));
//     assertTrue(put("600", "v600"));
//     assertTrue(put("900", "v900"));
//     TEST_CompactMemTable(dbfull());
//     assertEquals("2,1,1", FilesPerLevel());
//
//     // Compact away the placeholder files we created initially
//     dbfull()->TEST_CompactRange(1, NULL, NULL);
//     dbfull()->TEST_CompactRange(2, NULL, NULL);
//     assertEquals("2", FilesPerLevel());
//
//     // Do a memtable compaction.  Before bug-fix, the compaction would
//     // not detect the overlap with level-0 files and would incorrectly place
//     // the deletion in a deeper level.
//     assertOK(Delete("600"));
//     TEST_CompactMemTable(dbfull());
//     assertEquals("3", FilesPerLevel());
//     assertEquals("NOT_FOUND", get("600"));
        } while (changeOptions());
    }

/*@Test 33*/  public void DBTest_L0_CompactionBug_Issue44_a() {
//   Reopen();
//   assertTrue(put("b", "v"));
//   Reopen();
//   assertOK(Delete("b"));
//   assertOK(Delete("a"));
//   Reopen();
//   assertOK(Delete("a"));
//   Reopen();
//   assertTrue(put("a", "v"));
//   Reopen();
//   Reopen();
//   assertEquals("(a->v)", Contents());
//   DelayMilliseconds(1000);  // Wait for compaction to finish
//   assertEquals("(a->v)", Contents());
    }

/*@Test 34*/  public void DBTest_L0_CompactionBug_Issue44_b() {
//   Reopen();
//   Put("","");
//   Reopen();
//   Delete("e");
//   Put("","");
//   Reopen();
//   Put("c", "cv");
//   Reopen();
//   Put("","");
//   Reopen();
//   Put("","");
//   DelayMilliseconds(1000);  // Wait for compaction to finish
//   Reopen();
//   Put("d","dv");
//   Reopen();
//   Put("","");
//   Reopen();
//   Delete("d");
//   Delete("b");
//   Reopen();
//   assertEquals("(->)(c->cv)", Contents());
//   DelayMilliseconds(1000);  // Wait for compaction to finish
//   assertEquals("(->)(c->cv)", Contents());
    }

/*@Test 35*/  public void DBTest_ComparatorCheck() {
//   class NewComparator : public Comparator {
//    public:
//     virtual const char* Name() const { return "leveldb.NewComparator"; }
//     virtual int Compare(const Slice& a, const Slice& b) const {
//       return BytewiseComparator()->Compare(a, b);
//     }
//     virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
//       BytewiseComparator()->FindShortestSeparator(s, l);
//     }
//     virtual void FindShortSuccessor(std::string* key) const {
//       BytewiseComparator()->FindShortSuccessor(key);
//     }
//   };
//   NewComparator cmp;
//   Options new_options = CurrentOptions();
//   new_options.comparator = &cmp;
//   Status s = TryReopen(&new_options);
//   assertTrue(!s.ok());
//   assertTrue(s.ToString().find("comparator") != std::string::npos)
//       << s.ToString();
    }

/*@Test 36*/  public void DBTest_CustomComparator() {
//   class NumberComparator : public Comparator {
//    public:
//     virtual const char* Name() const { return "test.NumberComparator"; }
//     virtual int Compare(const Slice& a, const Slice& b) const {
//       return ToNumber(a) - ToNumber(b);
//     }
//     virtual void FindShortestSeparator(std::string* s, const Slice& l) const {
//       ToNumber(*s);     // Check format
//       ToNumber(l);      // Check format
//     }
//     virtual void FindShortSuccessor(std::string* key) const {
//       ToNumber(*key);   // Check format
//     }
//    private:
//     static int ToNumber(const Slice& x) {
//       // Check that there are no extra characters.
//       assertTrue(x.size() >= 2 && x[0] == '[' && x[x.size()-1] == ']')
//           << EscapeString(x);
//       int val;
//       char ignored;
//       assertTrue(sscanf(x.ToString().c_str(), "[%i]%c", &val, &ignored) == 1)
//           << EscapeString(x);
//       return val;
//     }
//   };
//   NumberComparator cmp;
//   Options new_options = CurrentOptions();
//   new_options.create_if_missing = true;
//   new_options.comparator = &cmp;
//   new_options.filter_policy = NULL;     // Cannot use bloom filters
//   new_options.write_buffer_size = 1000;  // Compact more often
//   DestroyAndReopen(&new_options);
//   assertTrue(put("[10]", "ten"));
//   assertTrue(put("[0x14]", "twenty"));
//   for (int i = 0; i < 2; i++) {
//     assertEquals("ten", get("[10]"));
//     assertEquals("ten", get("[0xa]"));
//     assertEquals("twenty", get("[20]"));
//     assertEquals("twenty", get("[0x14]"));
//     assertEquals("NOT_FOUND", get("[15]"));
//     assertEquals("NOT_FOUND", get("[0xf]"));
//     Compact("[0]", "[9999]");
//   }
//
//   for (int run = 0; run < 2; run++) {
//     for (int i = 0; i < 1000; i++) {
//       char buf[100];
//       snprintf(buf, sizeof(buf), "[%d]", i*10);
//       assertTrue(put(buf, buf));
//     }
//     Compact("[0]", "[1000000]");
//   }
    }

/*@Test 37*/  public void DBTest_ManualCompaction() {
//   assertEquals(config::kMaxMemCompactLevel, 2)
//       << "Need to update this test to match kMaxMemCompactLevel";
//
//   MakeTables(3, "p", "q");
//   assertEquals("1,1,1", FilesPerLevel());
//
//   // Compaction range falls before files
//   Compact("", "c");
//   assertEquals("1,1,1", FilesPerLevel());
//
//   // Compaction range falls after files
//   Compact("r", "z");
//   assertEquals("1,1,1", FilesPerLevel());
//
//   // Compaction range overlaps files
//   Compact("p1", "p9");
//   assertEquals("0,0,1", FilesPerLevel());
//
//   // Populate a different range
//   MakeTables(3, "c", "e");
//   assertEquals("1,1,2", FilesPerLevel());
//
//   // Compact just the new range
//   Compact("b", "f");
//   assertEquals("0,0,2", FilesPerLevel());
//
//   // Compact all
//   MakeTables(1, "a", "z");
//   assertEquals("0,1,2", FilesPerLevel());
//   db_->CompactRange(NULL, NULL);
//   assertEquals("0,0,1", FilesPerLevel());
    }

/*@Test 38*/  public void DBTest_DBOpen_Options() {
//   std::string dbname = test::TmpDir() + "/db_options_test";
//   DestroyDB(dbname, Options());
//
//   // Does not exist, and create_if_missing == false: error
//   DB* db = NULL;
//   Options opts;
//   opts.create_if_missing = false;
//   Status s = DB::Open(opts, dbname, &db);
//   assertTrue(strstr(s.ToString().c_str(), "does not exist") != NULL);
//   assertTrue(db == NULL);
//
//   // Does not exist, and create_if_missing == true: OK
//   opts.create_if_missing = true;
//   s = DB::Open(opts, dbname, &db);
//   assertOK(s);
//   assertTrue(db != NULL);
//
//   delete db;
//   db = NULL;
//
//   // Does exist, and error_if_exists == true: error
//   opts.create_if_missing = false;
//   opts.error_if_exists = true;
//   s = DB::Open(opts, dbname, &db);
//   assertTrue(strstr(s.ToString().c_str(), "exists") != NULL);
//   assertTrue(db == NULL);
//
//   // Does exist, and error_if_exists == false: OK
//   opts.create_if_missing = true;
//   opts.error_if_exists = false;
//   s = DB::Open(opts, dbname, &db);
//   assertOK(s);
//   assertTrue(db != NULL);
//
//   delete db;
//   db = NULL;
    }

/*@Test 39*/  public void DBTest_Locking() {
//   DB* db2 = NULL;
//   Status s = DB::Open(CurrentOptions(), dbname_, &db2);
//   assertTrue(!s.ok()) << "Locking did not prevent re-opening db";
    }
//
// // Check that number of files does not grow when we are out of space
/*@Test 40*/  public void DBTest_NoSpace() {
//   Options options = CurrentOptions();
//   options.env = env_;
//   Reopen(&options);
//
//   assertTrue(put("foo", "v1"));
//   assertEquals("v1", get("foo"));
//   Compact("a", "z");
//   const int num_files = CountFiles();
//   env_->no_space_.Release_Store(env_);   // Force out-of-space errors
//   for (int i = 0; i < 10; i++) {
//     for (int level = 0; level < config::kNumLevels-1; level++) {
//       dbfull()->TEST_CompactRange(level, NULL, NULL);
//     }
//   }
//   env_->no_space_.Release_Store(NULL);
//   ASSERT_LT(CountFiles(), num_files + 3);
    }

/*@Test 41*/  public void DBTest_NonWritableFileSystem() {
//   Options options = CurrentOptions();
//   options.write_buffer_size = 1000;
//   options.env = env_;
//   Reopen(&options);
//   assertTrue(put("foo", "v1"));
//   env_->non_writable_.Release_Store(env_);  // Force errors for new files
//   std::string big(100000, 'x');
//   int errors = 0;
//   for (int i = 0; i < 20; i++) {
//     fprintf(stderr, "iter %d; errors %d\n", i, errors);
//     if (!Put("foo", big).ok()) {
//       errors++;
//       DelayMilliseconds(100);
//     }
//   }
//   ASSERT_GT(errors, 0);
//   env_->non_writable_.Release_Store(NULL);
    }

/*@Test 42*/  public void DBTest_WriteSyncError() {
//   // Check that log sync errors cause the DB to disallow future writes.
//
//   // (a) Cause log sync calls to fail
//   Options options = CurrentOptions();
//   options.env = env_;
//   Reopen(&options);
//   env_->data_sync_error_.Release_Store(env_);
//
//   // (b) Normal write should succeed
//   WriteOptions w;
//   assertOK(db_->Put(w, "k1", "v1"));
//   assertEquals("v1", get("k1"));
//
//   // (c) Do a sync write; should fail
//   w.sync = true;
//   assertTrue(!db_->Put(w, "k2", "v2").ok());
//   assertEquals("v1", get("k1"));
//   assertEquals("NOT_FOUND", get("k2"));
//
//   // (d) make sync behave normally
//   env_->data_sync_error_.Release_Store(NULL);
//
//   // (e) Do a non-sync write; should fail
//   w.sync = false;
//   assertTrue(!db_->Put(w, "k3", "v3").ok());
//   assertEquals("v1", get("k1"));
//   assertEquals("NOT_FOUND", get("k2"));
//   assertEquals("NOT_FOUND", get("k3"));
    }

/*@Test 43*/  public void DBTest_ManifestWriteError() {
//   // Test for the following problem:
//   // (a) Compaction produces file F
//   // (b) Log record containing F is written to MANIFEST file, but Sync() fails
//   // (c) GC deletes F
//   // (d) After reopening DB, reads fail since deleted F is named in log record
//
//   // We iterate twice.  In the second iteration, everything is the
//   // same except the log record never makes it to the MANIFEST file.
//   for (int iter = 0; iter < 2; iter++) {
//     port::AtomicPointer* error_type = (iter == 0)
//         ? &env_->manifest_sync_error_
//         : &env_->manifest_write_error_;
//
//     // Insert foo=>bar mapping
//     Options options = CurrentOptions();
//     options.env = env_;
//     options.create_if_missing = true;
//     options.error_if_exists = false;
//     DestroyAndReopen(&options);
//     assertTrue(put("foo", "bar"));
//     assertEquals("bar", get("foo"));
//
//     // Memtable compaction (will succeed)
//     TEST_CompactMemTable(dbfull());
//     assertEquals("bar", get("foo"));
//     const int last = config::kMaxMemCompactLevel;
//     assertEquals(NumTableFilesAtLevel(last), 1);   // foo=>bar is now in last level
//
//     // Merging compaction (will fail)
//     error_type->Release_Store(env_);
//     dbfull()->TEST_CompactRange(last, NULL, NULL);  // Should fail
//     assertEquals("bar", get("foo"));
//
//     // Recovery: should not lose data
//     error_type->Release_Store(NULL);
//     Reopen(&options);
//     assertEquals("bar", get("foo"));
//   }
    }

/*@Test 44*/  public void DBTest_MissingSSTFile() {
//   assertTrue(put("foo", "bar"));
//   assertEquals("bar", get("foo"));
//
//   // Dump the memtable to disk.
//   TEST_CompactMemTable(dbfull());
//   assertEquals("bar", get("foo"));
//
//   Close();
//   assertTrue(DeleteAnSSTFile());
//   Options options = CurrentOptions();
//   options.paranoid_checks = true;
//   Status s = TryReopen(&options);
//   assertTrue(!s.ok());
//   assertTrue(s.ToString().find("issing") != std::string::npos)
//       << s.ToString();
    }

/*@Test 45*/  public void DBTest_StillReadSST() {
//   assertTrue(put("foo", "bar"));
//   assertEquals("bar", get("foo"));
//
//   // Dump the memtable to disk.
//   TEST_CompactMemTable(dbfull());
//   assertEquals("bar", get("foo"));
//   Close();
//   ASSERT_GT(RenameLDBToSST(), 0);
//   Options options = CurrentOptions();
//   options.paranoid_checks = true;
//   Status s = TryReopen(&options);
//   assertTrue(s.ok());
//   assertEquals("bar", get("foo"));
    }

/*@Test 46*/  public void DBTest_FilesDeletedAfterCompaction() {
//   assertTrue(put("foo", "v2"));
//   Compact("a", "z");
//   const int num_files = CountFiles();
//   for (int i = 0; i < 10; i++) {
//     assertTrue(put("foo", "v2"));
//     Compact("a", "z");
//   }
//   assertEquals(CountFiles(), num_files);
    }

/*@Test 47*/  public void DBTest_BloomFilter() {
//   env_->count_random_reads_ = true;
//   Options options = CurrentOptions();
//   options.env = env_;
//   options.block_cache = NewLRUCache(0);  // Prevent cache hits
//   options.filter_policy = NewBloomFilterPolicy(10);
//   Reopen(&options);
//
//   // Populate multiple layers
//   const int N = 10000;
//   for (int i = 0; i < N; i++) {
//     assertTrue(put(Key(i), Key(i)));
//   }
//   Compact("a", "z");
//   for (int i = 0; i < N; i += 100) {
//     assertTrue(put(Key(i), Key(i)));
//   }
//   TEST_CompactMemTable(dbfull());
//
//   // Prevent auto compactions triggered by seeks
//   env_->delay_data_sync_.Release_Store(env_);
//
//   // Lookup present keys.  Should rarely read from small sstable.
//   env_->random_read_counter_.Reset();
//   for (int i = 0; i < N; i++) {
//     assertEquals(Key(i), get(Key(i)));
//   }
//   int reads = env_->random_read_counter_.Read();
//   fprintf(stderr, "%d present => %d reads\n", N, reads);
//   ASSERT_GE(reads, N);
//   ASSERT_LE(reads, N + 2*N/100);
//
//   // Lookup present keys.  Should rarely read from either sstable.
//   env_->random_read_counter_.Reset();
//   for (int i = 0; i < N; i++) {
//     assertEquals("NOT_FOUND", get(Key(i) + ".missing"));
//   }
//   reads = env_->random_read_counter_.Read();
//   fprintf(stderr, "%d missing => %d reads\n", N, reads);
//   ASSERT_LE(reads, 3*N/100);
//
//   env_->delay_data_sync_.Release_Store(NULL);
//   Close();
//   delete options.block_cache;
//   delete options.filter_policy;
    }

// // Multi-threaded test:
// namespace {

// static const int kNumThreads = 4;
// static const int kTestSeconds = 10;
// static const int kNumKeys = 1000;
//
// struct MTState {
//   DBTest* test;
//   port::AtomicPointer stop;
//   port::AtomicPointer counter[kNumThreads];
//   port::AtomicPointer thread_done[kNumThreads];
// };
//
// struct MTThread {
//   MTState* state;
//   int id;
// };

// static void MTThreadBody(void* arg) {
//   MTThread* t = reinterpret_cast<MTThread*>(arg);
//   int id = t->id;
//   DB* db = t->state->test->db_;
//   uintptr_t counter = 0;
//   fprintf(stderr, "... starting thread %d\n", id);
//   Random rnd(1000 + id);
//   std::string value;
//   char valbuf[1500];
//   while (t->state->stop.Acquire_Load() == NULL) {
//     t->state->counter[id].Release_Store(reinterpret_cast<void*>(counter));
//
//     int key = rnd.Uniform(kNumKeys);
//     char keybuf[20];
//     snprintf(keybuf, sizeof(keybuf), "%016d", key);
//
//     if (rnd.OneIn(2)) {
//       // Write values of the form <key, my id, counter>.
//       // We add some padding for force compactions.
//       snprintf(valbuf, sizeof(valbuf), "%d.%d.%-1000d",
//                key, id, static_cast<int>(counter));
//       assertOK(db->Put(WriteOptions(), Slice(keybuf), Slice(valbuf)));
//     } else {
//       // Read a value and verify that it matches the pattern written above.
//       Status s = db->Get(ReadOptions(), Slice(keybuf), &value);
//       if (s.IsNotFound()) {
//         // Key has not yet been written
//       } else {
//         // Check that the writer thread counter is >= the counter in the value
//         assertOK(s);
//         int k, w, c;
//         assertEquals(3, sscanf(value.c_str(), "%d.%d.%d", &k, &w, &c)) << value;
//         assertEquals(k, key);
//         ASSERT_GE(w, 0);
//         ASSERT_LT(w, kNumThreads);
//         ASSERT_LE(static_cast<uintptr_t>(c), reinterpret_cast<uintptr_t>(
//             t->state->counter[w].Acquire_Load()));
//       }
//     }
//     counter++;
//   }
//   t->state->thread_done[id].Release_Store(t);
//   fprintf(stderr, "... stopping thread %d after %d ops\n", id, int(counter));
// }

// } // namespace

/*@Test 48*/  public void DBTest_MultiThreaded() {
        do {
//     // Initialize state
//     MTState mt;
//     mt.test = this;
//     mt.stop.Release_Store(0);
//     for (int id = 0; id < kNumThreads; id++) {
//       mt.counter[id].Release_Store(0);
//       mt.thread_done[id].Release_Store(0);
//     }
//
//     // Start threads
//     MTThread thread[kNumThreads];
//     for (int id = 0; id < kNumThreads; id++) {
//       thread[id].state = &mt;
//       thread[id].id = id;
//       env_->StartThread(MTThreadBody, &thread[id]);
//     }
//
//     // Let them run for a while
//     DelayMilliseconds(kTestSeconds * 1000);
//
//     // Stop the threads and wait for them to finish
//     mt.stop.Release_Store(&mt);
//     for (int id = 0; id < kNumThreads; id++) {
//       while (mt.thread_done[id].Acquire_Load() == NULL) {
//         DelayMilliseconds(100);
//       }
//     }
        } while (changeOptions());
    }

// namespace {
// typedef std::map<std::string, std::string> KVMap;
// }

// class ModelDB: public DB {
//  public:
//   class ModelSnapshot : public Snapshot {
//    public:
//     KVMap map_;
//   };
//
//   explicit ModelDB(const Options& options): options_(options) { }
//   ~ModelDB() { }
//   virtual Status Put(const WriteOptions& o, const Slice& k, const Slice& v) {
//     return DB::Put(o, k, v);
//   }
//   virtual Status Delete(const WriteOptions& o, const Slice& key) {
//     return DB::Delete(o, key);
//   }
//   virtual Status Get(const ReadOptions& options,
//                      const Slice& key, std::string* value) {
//     assert(false);      // Not implemented
//     return Status::NotFound(key);
//   }
//   virtual Iterator* NewIterator(const ReadOptions& options) {
//     if (options.snapshot == NULL) {
//       KVMap* saved = new KVMap;
//       *saved = map_;
//       return new ModelIter(saved, true);
//     } else {
//       const KVMap* snapshot_state =
//           &(reinterpret_cast<const ModelSnapshot*>(options.snapshot)->map_);
//       return new ModelIter(snapshot_state, false);
//     }
//   }
//   virtual const Snapshot* GetSnapshot() {
//     ModelSnapshot* snapshot = new ModelSnapshot;
//     snapshot->map_ = map_;
//     return snapshot;
//   }
//
//   virtual void ReleaseSnapshot(const Snapshot* snapshot) {
//     delete reinterpret_cast<const ModelSnapshot*>(snapshot);
//   }
//   virtual Status Write(const WriteOptions& options, WriteBatch* batch) {
//     class Handler : public WriteBatch::Handler {
//      public:
//       KVMap* map_;
//       virtual void Put(const Slice& key, const Slice& value) {
//         (*map_)[key.ToString()] = value.ToString();
//       }
//       virtual void Delete(const Slice& key) {
//         map_->erase(key.ToString());
//       }
//     };
//     Handler handler;
//     handler.map_ = &map_;
//     return batch->Iterate(&handler);
//   }
//
//   virtual bool GetProperty(const Slice& property, std::string* value) {
//     return false;
//   }
//   virtual void GetApproximateSizes(const Range* r, int n, uint64_t* sizes) {
//     for (int i = 0; i < n; i++) {
//       sizes[i] = 0;
//     }
//   }
//   virtual void CompactRange(const Slice* start, const Slice* end) {
//   }
//
//  private:
//   class ModelIter: public Iterator {
//    public:
//     ModelIter(const KVMap* map, bool owned)
//         : map_(map), owned_(owned), iter_(map_->end()) {
//     }
//     ~ModelIter() {
//       if (owned_) delete map_;
//     }
//     virtual bool Valid() const { return iter_ != map_->end(); }
//     virtual void SeekToFirst() { iter_ = map_->begin(); }
//     virtual void SeekToLast() {
//       if (map_->empty()) {
//         iter_ = map_->end();
//       } else {
//         iter_ = map_->find(map_->rbegin()->first);
//       }
//     }
//     virtual void Seek(const Slice& k) {
//       iter_ = map_->lower_bound(k.ToString());
//     }
//     virtual void Next() { ++iter_; }
//     virtual void Prev() { --iter_; }
//     virtual Slice key() const { return iter_->first; }
//     virtual Slice value() const { return iter_->second; }
//     virtual Status status() const { return Status::OK(); }
//    private:
//     const KVMap* const map_;
//     const bool owned_;  // Do we own map_
//     KVMap::const_iterator iter_;
//   };
//   const Options options_;
//   KVMap map_;
// };

// static std::string RandomKey(Random* rnd) {
//   int len = (rnd->OneIn(3)
//              ? 1                // Short sometimes to encourage collisions
//              : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
//   return test::RandomKey(rnd, len);
// }

// static bool CompareIterators(int step,
//                              DB* model,
//                              DB* db,
//                              const Snapshot* model_snap,
//                              const Snapshot* db_snap) {
//   ReadOptions options;
//   options.snapshot = model_snap;
//   Iterator* miter = model->NewIterator(options);
//   options.snapshot = db_snap;
//   Iterator* dbiter = db->NewIterator(options);
//   bool ok = true;
//   int count = 0;
//   for (miter->SeekToFirst(), dbiter->SeekToFirst();
//        ok && miter->Valid() && dbiter->Valid();
//        miter->Next(), dbiter->Next()) {
//     count++;
//     if (miter->key().compare(dbiter->key()) != 0) {
//       fprintf(stderr, "step %d: Key mismatch: '%s' vs. '%s'\n",
//               step,
//               EscapeString(miter->key()).c_str(),
//               EscapeString(dbiter->key()).c_str());
//       ok = false;
//       break;
//     }
//
//     if (miter->value().compare(dbiter->value()) != 0) {
//       fprintf(stderr, "step %d: Value mismatch for key '%s': '%s' vs. '%s'\n",
//               step,
//               EscapeString(miter->key()).c_str(),
//               EscapeString(miter->value()).c_str(),
//               EscapeString(miter->value()).c_str());
//       ok = false;
//     }
//   }
//
//   if (ok) {
//     if (miter->Valid() != dbiter->Valid()) {
//       fprintf(stderr, "step %d: Mismatch at end of iterators: %d vs. %d\n",
//               step, miter->Valid(), dbiter->Valid());
//       ok = false;
//     }
//   }
//   fprintf(stderr, "%d entries compared: ok=%d\n", count, ok);
//   delete miter;
//   delete dbiter;
//   return ok;
// }

/*@Test 49*/  public void DBTest_Randomized() {
//   Random rnd(test::RandomSeed());
        do {
//     ModelDB model(CurrentOptions());
//     const int N = 10000;
//     const Snapshot* model_snap = NULL;
//     const Snapshot* db_snap = NULL;
//     std::string k, v;
//     for (int step = 0; step < N; step++) {
//       if (step % 100 == 0) {
//         fprintf(stderr, "Step %d of %d\n", step, N);
//       }
//       // TODO(sanjay): Test Get() works
//       int p = rnd.Uniform(100);
//       if (p < 45) {                               // Put
//         k = RandomKey(&rnd);
//         v = RandomString(&rnd,
//                          rnd.OneIn(20)
//                          ? 100 + rnd.Uniform(100)
//                          : rnd.Uniform(8));
//         assertOK(model.Put(WriteOptions(), k, v));
//         assertOK(db_->Put(WriteOptions(), k, v));
//
//       } else if (p < 90) {                        // Delete
//         k = RandomKey(&rnd);
//         assertOK(model.Delete(WriteOptions(), k));
//         assertOK(db_->Delete(WriteOptions(), k));
//
//
//       } else {                                    // Multi-element batch
//         WriteBatch b;
//         const int num = rnd.Uniform(8);
//         for (int i = 0; i < num; i++) {
//           if (i == 0 || !rnd.OneIn(10)) {
//             k = RandomKey(&rnd);
//           } else {
//             // Periodically re-use the same key from the previous iter, so
//             // we have multiple entries in the write batch for the same key
//           }
//           if (rnd.OneIn(2)) {
//             v = RandomString(&rnd, rnd.Uniform(10));
//             b.Put(k, v);
//           } else {
//             b.Delete(k);
//           }
//         }
//         assertOK(model.Write(WriteOptions(), &b));
//         assertOK(db_->Write(WriteOptions(), &b));
//       }
//
//       if ((step % 100) == 0) {
//         assertTrue(CompareIterators(step, &model, db_, NULL, NULL));
//         assertTrue(CompareIterators(step, &model, db_, model_snap, db_snap));
//         // Save a snapshot from each DB this time that we'll use next
//         // time we compare things, to make sure the current state is
//         // preserved with the snapshot
//         if (model_snap != NULL) model.ReleaseSnapshot(model_snap);
//         if (db_snap != NULL) db_->ReleaseSnapshot(db_snap);
//
//         Reopen();
//         assertTrue(CompareIterators(step, &model, db_, NULL, NULL));
//
//         model_snap = model.GetSnapshot();
//         db_snap = db_->GetSnapshot();
//       }
//     }
//     if (model_snap != NULL) model.ReleaseSnapshot(model_snap);
//     if (db_snap != NULL) db_->ReleaseSnapshot(db_snap);
        } while (changeOptions());
    }

// std::string MakeKey(unsigned int num) {
//   char buf[30];
//   snprintf(buf, sizeof(buf), "%016u", num);
//   return std::string(buf);
// }

// void BM_LogAndApply(int iters, int num_base_files) {
//   std::string dbname = test::TmpDir() + "/leveldb_test_benchmark";
//   DestroyDB(dbname, Options());
//
//   DB* db = NULL;
//   Options opts;
//   opts.create_if_missing = true;
//   Status s = DB::Open(opts, dbname, &db);
//   assertOK(s);
//   assertTrue(db != NULL);
//
//   delete db;
//   db = NULL;
//
//   Env* env = Env::Default();
//
//   port::Mutex mu;
//   MutexLock l(&mu);
//
//   InternalKeyComparator cmp(BytewiseComparator());
//   Options options;
//   VersionSet vset(dbname, &options, NULL, &cmp);
//   bool save_manifest;
//   assertOK(vset.Recover(&save_manifest));
//   VersionEdit vbase;
//   uint64_t fnum = 1;
//   for (int i = 0; i < num_base_files; i++) {
//     InternalKey start(MakeKey(2*fnum), 1, kTypeValue);
//     InternalKey limit(MakeKey(2*fnum+1), 1, kTypeDeletion);
//     vbase.AddFile(2, fnum++, 1 /* file size */, start, limit);
//   }
//   assertOK(vset.LogAndApply(&vbase, &mu));
//
//   uint64_t start_micros = env->NowMicros();
//
//   for (int i = 0; i < iters; i++) {
//     VersionEdit vedit;
//     vedit.DeleteFile(2, fnum);
//     InternalKey start(MakeKey(2*fnum), 1, kTypeValue);
//     InternalKey limit(MakeKey(2*fnum+1), 1, kTypeDeletion);
//     vedit.AddFile(2, fnum++, 1 /* file size */, start, limit);
//     vset.LogAndApply(&vedit, &mu);
//   }
//   uint64_t stop_micros = env->NowMicros();
//   unsigned int us = stop_micros - start_micros;
//   char buf[16];
//   snprintf(buf, sizeof(buf), "%d", num_base_files);
//   fprintf(stderr,
//           "BM_LogAndApply/%-6s   %8d iters : %9u us (%7.0f us / iter)\n",
//           buf, iters, us, ((float)us) / iters);
// }


}

/*
  int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--benchmark") {
      leveldb::BM_LogAndApply(1000, 1);
      leveldb::BM_LogAndApply(1000, 100);
      leveldb::BM_LogAndApply(1000, 10000);
      leveldb::BM_LogAndApply(100, 100000);
      return 0;
    }

    return leveldb::test::RunAllTests();
  }
*/
