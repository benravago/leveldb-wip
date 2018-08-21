package bsd.leveldb.db;

import java.util.List;
import java.util.ArrayList;
import java.util.Formatter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.channels.FileLock;

import bsd.leveldb.Slice;
import bsd.leveldb.Cursor;
import bsd.leveldb.Options;
import bsd.leveldb.Status;
import bsd.leveldb.ReadOptions;
import bsd.leveldb.io.Escape;
import bsd.leveldb.io.MutexLock;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.FileName.*;

class DbUtil {

    static class IterState {
        MutexLock mu;
        Version version;
        MemTable mem;
        MemTable imm;

        Cursor<InternalKey,Slice> iter;
        long latestSnapshot;
        int seed;
    }

    static Cursor<Slice, Slice> newIterator(DbMain db, ReadOptions options) {
        db.mutex.lock();
        try {
            IterState state = newInternalIterator(db,options.fillCache);
            return newDbIterator(
                db, state.iter,
                (options.snapshot != null
                    ? db.lookup(options.snapshot)
                    : state.latestSnapshot),
                state.seed);
        }
        finally {
            db.mutex.unlock();
        }
    }

    // Return a new iterator that converts internal keys (yielded by "*internal_iter")
    // that were live at the specified "sequence" number into appropriate user keys.
    static Cursor<Slice,Slice> newDbIterator(DbMain db, Cursor<InternalKey,Slice> internalIter, long sequence, int seed) {
        return new DbIter( internalIter, sequence, db::recordReadSample, seed );
    }

    static IterState newInternalIterator(DbMain db, boolean fillCache) {
        assert (db.mutex.isHeldByCurrentThread());
        IterState cleanup = new IterState();
        cleanup.latestSnapshot = db.versions.lastSequence();

        // Collect together all needed child iterators
        List<Cursor<InternalKey,Slice>> list = new ArrayList<>();
        list.add(db.memTable.newIterator());
        db.memTable.ref();
        if (db.immuTable != null) {
            list.add(db.immuTable.newIterator());
            db.immuTable.ref();
        }
        db.versions.current().addIterators(fillCache,list);

        cleanup.mu = db.mutex;
        cleanup.mem = db.memTable;
        cleanup.imm = db.immuTable;
        cleanup.version = db.versions.current();

        cleanup.iter =
            new MergingIterator(db.internalComparator, list.toArray(new Cursor[list.size()])) {
                @Override
                public void close() {
                    cleanupIteratorState(cleanup);
                }
                // internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);
            };
        db.versions.current().ref();
        cleanup.seed = ++db.seed;
        return cleanup;
    }

    static void cleanupIteratorState(IterState state) {
        state.mu.lock();
        try {
            state.mem.unref();
            if (state.imm != null) state.imm.unref();
            state.version.unref();
        }
        finally {
            state.mu.unlock();
        }
        // delete state;
    }

    static void repairDB(Options options, String name) {
        // Repair.run(name, options);
    }

    static void destroyDB(Options options, String name) {
        Env env = options.env != null ? options.env : new Env(){};
        Path dbname = Paths.get(name);
        Path[] filenames =
            env.getChildren(dbname);
        if (filenames == null || filenames.length == 0) {
            // Ignore error in case directory does not exist
            return;
        }
        try {
            FileLock lock;
            Path lockname = lockFileName(dbname);
            lock = env.lockFile(lockname);
            for (Path filename : filenames) {
                ParsedFileName pfn = parseFileName(filename);
                if (pfn.type != FileType.kDBLockFile) {
                    // Lock file will be deleted at end
                    env.deleteFile(filename);
                }
            }
            env.unlockFile(lock);  // Ignore error since state is already gone
            env.deleteFile(lockname);
            env.deleteDir(dbname);  // Ignore error in case dir contains other files
        }
        catch (IOException e) { throw new Status(e).state(Status.Code.IOError); }
    }

    static long getApproximateSize(DbImpl db, Slice startKey, Slice limitKey) {
        Version v;
        try (MutexLock l = db.mutex.open()) { // MutexLock l(&mutex_);
            db.versions.current().ref();
            v = db.versions.current();
        }

        // Convert user_key into a corresponding internal key.
        InternalKey k1 = internalKey(startKey, kMaxSequenceNumber, kValueTypeForSeek);
        InternalKey k2 = internalKey(limitKey, kMaxSequenceNumber, kValueTypeForSeek);
        long start = db.versions.approximateOffsetOf(v, k1);
        long limit = db.versions.approximateOffsetOf(v, k2);
        long size = (limit >= start ? limit - start : 0);

        try (MutexLock l = db.mutex.open()) { // MutexLock l(&mutex_);
            v.unref();
        }
        return size;
    }

    static Object getProperty(DbImpl db, String property) {
      try (MutexLock l = db.mutex.open()) {
        if (property.startsWith("leveldb.num-files-at-level")) {
            return getNumFilesAtLevel(db,Integer.parseInt(property.substring(26)));
        }
        switch (property) {
            case "leveldb.sstables":                 return getSSTables(db);
            case "leveldb.compaction-stats":         return getCompactionStats(db);
            case "leveldb.approximate-memory-usage": return getApproximateMemoryUsage(db);
            case "leveldb.implementation":           return (Object)db;
            default: return null;
        }
      }
    }

    static String getNumFilesAtLevel(DbMain db, int level) {
        return level > kNumLevels ? null
             : Integer.toString(db.versions.numLevelFiles(level));
    }

    static String getSSTables(DbMain db) {
        return null; // TODO: Debug.string(db.versions.current());
    }

    static String getCompactionStats(DbImpl db) {
        Formatter f = new Formatter();
        f.format("Compactions\n")
         .format("Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n")
         .format("--------------------------------------------------\n");
        for (int level = 0; level < kNumLevels; level++) {
            DbImpl.CompactionStats stats = db.stats[level];
            int files = db.versions.numLevelFiles(level);
            if (stats.micros > 0 || files > 0) {
                f.format("%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                         level,
                         files,
                         db.versions.numLevelBytes(level) / 1048576.0,
                         stats.micros / 1e6,
                         stats.bytesRead / 1048576.0,
                         stats.bytesWritten / 1048576.0);
            }
        }
        return f.toString();
    }

    static String getApproximateMemoryUsage(DbMain db) {
        // size_t total_usage = options_.block_cache->TotalCharge();
        long totalUsage = db.tableCache.approximateMemoryUsage();
        if (db.memTable != null) {
            totalUsage += db.memTable.approximateMemoryUsage();
        }
        if (db.immuTable != null) {
            totalUsage += db.immuTable.approximateMemoryUsage();
        }
        return Long.toString(totalUsage);
    }

    static String string(InternalKey k) {
        return String.format("%s @ %d : %d",
            string(k.userKey),
            sequenceNumber(k),
            valueType(k));
    }

    static String string(Slice s) {
        String c = Escape.chars(s.data,s.offset,s.length).toString();
        if (c.contains("\"")) {
            c = c.replaceAll("\"","`22`").replaceAll("``","`");
        }
        return "\""+c+"\"";
    }

    static String string(Version v) { // TODO:
        return null;
    }

}
