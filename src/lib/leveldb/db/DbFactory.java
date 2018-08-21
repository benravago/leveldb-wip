package bsd.leveldb.db;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Comparator;

import bsd.leveldb.DB;
import bsd.leveldb.Options;
import bsd.leveldb.ReadOptions;
import bsd.leveldb.Cursor;
import bsd.leveldb.Slice;
import bsd.leveldb.Snapshot;
import bsd.leveldb.Status;
import bsd.leveldb.WriteBatch;
import bsd.leveldb.WriteOptions;
import bsd.leveldb.io.Info;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.FileName.*;


public class DbFactory {

    static final int kNumNonTableCacheFiles = 10;

    /**
     * Fix user-supplied options to be reasonable
     */
    static int clipToRange(int value, int minvalue, int maxvalue) {
        return (value > maxvalue) ? maxvalue
             : (value < minvalue) ? minvalue
             :  value ;
    }

    /**
     * Sanitize db options; into a new Options instance.
     */
    static void sanitizeOptions(DbImpl db, Options src, String dbname) throws IOException {

        db.dbname = Paths.get(dbname);
        db.env = environment(src.env);

        db.internalComparator = internalComparator(src.comparator);
        db.filterPolicy = src.filterPolicy;
        db.compression = src.compression.k;

        db.createIfMissing = src.createIfMissing;
        db.errorIfExists = src.errorIfExists;
        db.reuseLogs = src.reuseLogs;
        db.paranoidChecks = src.paranoidChecks;

        int maxOpenFiles = clipToRange(src.maxOpenFiles, 64 + kNumNonTableCacheFiles, 50000 );
        int maxFileSize = clipToRange(src.maxFileSize, 1 << 20, 1 << 30 );

        db.writeBufferSize = clipToRange(src.writeBufferSize, 64 << 10, 1 << 30 );
        db.blockSize = clipToRange(src.blockSize, 1 << 10, 4 << 20 );
        db.blockRestartInterval = src.blockRestartInterval;

        if (src.infoLog != null) {
            Info.setLogger(src.infoLog);
        } else {
            // Open a log file in the same directory as the db
            db.env.createDir(db.dbname);  // In case it does not exist
            Path lfile = infoLogFileName(db.dbname);
            db.env.renameFile(lfile,oldInfoLogFileName(db.dbname));
            Info.setLogger(lfile,db.env.newAppendableFile(lfile));
            db.ownsInfoLog = true;
        }

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = maxOpenFiles - kNumNonTableCacheFiles;
        int blockCacheSize = src.blockCacheSize / src.blockSize;

        db.tableCache =
            new TableCache(db.dbname,db.env)
                .comparator(db.internalComparator)
                .filterPolicy(db.filterPolicy)
                .verifyChecksums(src.paranoidChecks)
                .cache(blockCacheSize,tableCacheSize)
                .open();

        db.versions =
            new VersionSet(db.dbname,db.env)
                .comparator(db.internalComparator)
                .paranoidChecks(src.paranoidChecks)
                .files(db.reuseLogs,maxFileSize)
                .cache(db.tableCache)
                .open();
    }

    static Env environment(Env env) {
        return (env != null) ? env : new Env(){};
    }
    
    static InternalKeyComparator internalComparator(Comparator<Slice> c) {
        KeyComparator icmp = (c != null) ? keyComparator(c) : new BytewiseComparator();
        return new InternalKeyComparator(icmp);
    }

    static KeyComparator<Slice> keyComparator(Comparator<Slice> c) {
        return new KeyComparator<Slice>() {
            @Override public String name() { return c.getClass().getName(); }
            @Override public int compare(Slice a, Slice b) { return c.compare(a,b); }
        };
    }

    public static DB openDB(Options options, String name) {
        try {
            DbImpl impl = new DbImpl();
            sanitizeOptions(impl,options,name);
            impl.open();
            return stub(impl);
        }
        catch (IOException e) {
            throw new Status(e).state(Status.Code.IOError);
        }
    }

    static void closeDB(DbImpl db) {
        try {
            db.close();
        }
        catch (IOException e) {
            throw new Status(e).state(Status.Code.IOError);
        }
    }

    static DB stub(DbImpl i) {
      return new DB() {
        @Override
        public void close() { closeDB(i); }
        @Override
        public void put(WriteOptions o, Slice k, Slice v) { i.put(o,k,v); }
        @Override
        public void delete(WriteOptions o, Slice k) { i.delete(o,k); }
        @Override
        public void write(WriteOptions o, WriteBatch u) { i.write(o,u); }
        @Override
        public Slice get(ReadOptions o, Slice k) { return i.get(o,k); }
        @Override
        public Snapshot getSnapshot() { return i.getSnapshot(); }
        @Override
        public void releaseSnapshot(Snapshot s) { i.releaseSnapshot(s); }
        @Override
        public void compactRange(Slice b, Slice e) { i.compactRange(b,e); }
        @Override
        public Cursor<Slice,Slice> iterator(ReadOptions o) { return DbUtil.newIterator(i,o);}
        @Override
        public long getApproximateSize(Slice b, Slice e) { return DbUtil.getApproximateSize(i,b,e); }
        @Override
        public <T> T getProperty(String n) { return (T) DbUtil.getProperty(i,n); }
      };
    }

    public static WriteBatch newWriteBatch() { return new Batch.Que(); }
    public static FilterPolicy newBloomFilterPolicy(int bpp) { return new BloomFilterPolicy(bpp); }

    public static void repairDB(Options opt, String dbname) { DbUtil.repairDB(opt,dbname); }
    public static void destroyDB(Options opt, String dbname) { DbUtil.destroyDB(opt,dbname); }

}