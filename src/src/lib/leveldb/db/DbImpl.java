package lib.leveldb.db;

import java.nio.file.Path;
import java.nio.channels.FileLock;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.ArrayList;

import java.util.logging.Level;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import lib.util.logging.Log;
import lib.util.concurrent.MutexLock;

import lib.leveldb.Env;
import lib.leveldb.Slice;
import lib.leveldb.Cursor;
import lib.leveldb.DB.Snapshot;
import lib.leveldb.DB.FilterPolicy;
import static lib.leveldb.db.DbUtil.*;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.FileName.*;
import lib.util.logging.OutputHandler;

class DbImpl {

    // Constant after construction
    Env env;
    Path dbname;

    // const Options options_;

    boolean errorIfExists;
    boolean createIfMissing;
    boolean reuseLogs;
    boolean paranoidChecks;

    int writeBufferSize;
    int blockRestartInterval;

    int compression;
    int blockSize;

    // table_cache_ provides its own synchronization
    TableCache tableCache;

    // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
    FileLock dbLock = null;

    // State below is protected by mutex_
    final MutexLock mutex = new MutexLock();
    AtomicBoolean shuttingDown = new AtomicBoolean();
    Condition bgCv = mutex.newCondition(); // Signalled when background work finishes

    // Has a background compaction been scheduled or is running?
    AtomicBoolean bgCompactionScheduled = new AtomicBoolean();

    // Have we encountered a background error in paranoid mode?
    Exception bgError; // Status bg_error_;

    MemTable memTable = null; // mem_
    MemTable immuTable = null; // imm_ Memtable being compacted
    AtomicReference has_imm = new AtomicReference(immuTable);  // So bg thread can detect non-NULL imm_

    // Set of table files to protect from deletion because they are part of ongoing compactions.
    Set<Long> pendingOutputs = new HashSet<>();

    // TODO: maybe has_imm should be AtomicBoolean

    // WritableFile* logfile_;
    long logfileNumber = 0;
    LogWriter log = null;
    int seed;  // For sampling.

    FilterPolicy filterPolicy;

    InternalKeyComparator internalComparator;

    KeyComparator<Slice> userComparator() {
        return internalComparator.userComparator;
    }

    Log infoLog;
    OutputHandler infoStream;

    void info(String msg, Object... params) {
        infoLog.log(Level.INFO,null,msg,params);
    }

    VersionSet versions;

    Map<Snapshot,Long> snapshots = new LinkedHashMap<>();  // insert-order

    long snapshotsOldestNumber() {
        return snapshots.entrySet().iterator().next().getValue();
    }

    Snapshot getSnapshot() {
        mutex.open();
        try (mutex) {
            var key = new Snapshot(){};
            var value = versions.lastSequence();
            snapshots.put(key,value);
            return key;
        }
    }

    void releaseSnapshot(Snapshot key) {
        mutex.open();
        try (mutex) {
            verify(key);
            snapshots.remove(key);
        }
    }

    long lookup(Snapshot key) {
        verify(key);
        return snapshots.get(key);
    }

    void verify(Snapshot key) {
        if (!snapshots.containsKey(key)) {
            throw new IllegalArgumentException("not a Snapshot: "+key);
        }
    }

    void newDB() throws IOException { // void newDB() throws IOException {
        var newDb = new VersionEdit();
        newDb.setComparatorName(userComparator().name());
        newDb.setLogNumber(0);
        newDb.setNextFile(2);
        newDb.setLastSequence(0);

        var manifest = descriptorFileName(dbname,1);
        var file = env.newWritableFile(manifest);
        var newLog = new LogWriter(file);
        var record = newDb.encodeTo();
        newLog.addRecord(new Slice(record));
        newLog.close();

        // Make "CURRENT" file that points to the new manifest file.
        setCurrentFile(1);
    }

    // Delete any unneeded files and stale in-memory entries.

    void deleteObsoleteFiles() { // void deleteObsoleteFiles() {
        if (bgError != null) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        var live = new HashSet<Long>(pendingOutputs);
        versions.addLiveFiles(live);

        var filenames = env.getChildren(dbname);

        for (var filename : filenames) {
            var p = parseFileName(filename);
            if (p == null) continue; // if (ParseFileName(filenames[i], &number, &type)) {
            var keep = true;
            var number = p.number;
            var type = p.type;
            switch (type) {
                case kLogFile:
                    keep = ((number >= versions.logNumber()) ||
                            (number == versions.prevLogNumber()));
                    break;
                case kDescriptorFile:
                    // Keep my manifest file, and any newer incarnations'
                    // (in case there is a race that allows other incarnations)
                    keep = (number >= versions.manifestFileNumber());
                    break;
                case kTableFile:
                    keep = live.contains(number);
                    break;
                case kTempFile:
                    // Any temp files that are currently being written to must
                    // be recorded in pending_outputs_, which is inserted into "live"
                    keep = live.contains(number);
                    break;
                case kCurrentFile:
                case kDBLockFile:
                case kInfoLogFile:
                    keep = true;
                    break;
            }

            if (!keep) {
                if (type == FileName.FileType.kTableFile) {
                    tableCache.evict(number);
                }
                info("Delete {0} #{1,number}", type.name(), number );
                try {
                    env.deleteFile(filename);
                }
                catch (IOException e) {
                    throw ioerror(e);
                }
            }
        }
    }

    /**
     * Make the CURRENT file point to the descriptor file with the specified number.
     */
    void setCurrentFile(long descriptorNumber) throws IOException {
        // Remove leading "dbname/" and add newline to manifest file name
        var manifest = descriptorFileName(dbname,descriptorNumber);
        var contents = manifest.getFileName().toString() + '\n';
        var tmp = tempFileName(dbname, descriptorNumber);
        try {
            env.writeToFile(tmp,contents.getBytes());
            env.renameFile(tmp,currentFileName(dbname));
        }
        catch (IOException ioe) {
            env.deleteFile(tmp);
            throw ioe;
        }
    }

    /**
     *  Read "CURRENT" file, which contains a pointer to the current manifest file.
     */
    Path getCurrentFile() {
        try {
            var fname = currentFileName(dbname);
            var dscbase = env.readFromFile(fname);
            if (dscbase.length == 0 || dscbase[dscbase.length-1] != '\n') {
                throw corruption("CURRENT file does not end with newline");
            }
            return dbname.resolve(new String(dscbase).trim());
        }
        catch (IOException e) {
            throw ioerror(e);
        }
    }

    class IterState {
        MutexLock mu;
        Version version;
        MemTable mem, imm;
        Cursor<InternalKey,Slice> iter;
        long latestSnapshot;
        int seed;
    }

    Cursor<Slice, Slice> newIterator( Snapshot snapshot, boolean fillCache, boolean verifyChecksums) {
        mutex.lock();
        try (mutex) {
            var state = newInternalIterator(fillCache);
            return new DbIter(
                state.iter,
                (snapshot != null ? lookup(snapshot) : state.latestSnapshot),
                this::recordReadSample,
                state.seed);
        }
    }

    // no-op here
    void recordReadSample(InternalKey key) {}

    // Return a new iterator that converts internal keys (yielded by "*internal_iter")
    // that were live at the specified "sequence" number into appropriate user keys.
//  static Cursor<Slice,Slice> newDbIterator(Cursor<InternalKey,Slice> internalIter, long sequence, Consumer<InternalKey> keyConsumer, int seed) {
//      return new DbIter(internalIter,sequence,keyConsumer,seed);
//  }

    IterState newInternalIterator(boolean fillCache) {
        assert (mutex.isHeldByCurrentThread());
        var cleanup = new IterState();
        cleanup.latestSnapshot = versions.lastSequence();

        // Collect together all needed child iterators
        var list = new ArrayList<Cursor<InternalKey,Slice>>();
        list.add(memTable.newIterator());
        memTable.ref();
        if (immuTable != null) {
            list.add(immuTable.newIterator());
            immuTable.ref();
        }
        versions.current().addIterators(fillCache,list);

        cleanup.mu = mutex;
        cleanup.mem = memTable;
        cleanup.imm = immuTable;
        cleanup.version = versions.current();

        cleanup.iter =
            new MergingIterator(internalComparator, list.toArray(new Cursor[list.size()])) {
                @Override
                public void close() {
                    cleanupIteratorState(cleanup);
                }
                // internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);
            };
        versions.current().ref();
        cleanup.seed = ++seed;
        return cleanup;
    }

    void cleanupIteratorState(IterState state) {
        try (var l = state.mu.open()) {
            state.mem.unref();
            if (state.imm != null) state.imm.unref();
            state.version.unref();
        }
        // delete state;
    }

}
