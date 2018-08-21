package bsd.leveldb.db;

import bsd.leveldb.Cursor;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.nio.file.Path;
import java.nio.channels.FileLock;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.HashSet;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import bsd.leveldb.ReadOptions;
import bsd.leveldb.Slice;
import bsd.leveldb.Snapshot;
import bsd.leveldb.Status;
import bsd.leveldb.WriteBatch;
import bsd.leveldb.WriteOptions;
import bsd.leveldb.io.Info;
import bsd.leveldb.io.MutexLock;
import static bsd.leveldb.db.Struct.*;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.FileName.*;
import static bsd.leveldb.Status.Code.*;

class DbMain {

    // Constant after construction
    Env env;
    Path dbname;

    InternalKeyComparator internalComparator;
    FilterPolicy filterPolicy;

    // const Options options_;

    boolean errorIfExists;
    boolean createIfMissing;
    boolean reuseLogs;
    boolean paranoidChecks;

    int writeBufferSize;
    int blockRestartInterval;

    int compression;
    int blockSize;

    // TODO: Consumer<Throwable> checkStatus;

    // table_cache_ provides its own synchronization
    TableCache tableCache;

    // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
    FileLock dbLock = null;

    // State below is protected by mutex_
    MutexLock mutex = new MutexLock();
    AtomicBoolean shuttingDown = new AtomicBoolean();
    Condition bgCv = mutex.newCondition(); // Signalled when background work finishes
    MemTable memTable = null; // mem_
    MemTable immuTable = null; // imm_ Memtable being compacted
    AtomicReference has_imm = new AtomicReference(immuTable);  // So bg thread can detect non-NULL imm_
    // WritableFile* logfile_;
    long logfileNumber = 0;
    LogWriter log = null;
    int seed;  // For sampling.

    // Queue of writers.
    Deque<Waiter> writers = new LinkedList<>(); // std::deque<Writer*> writers_;
    Batch.Que tmpBatch = new Batch.Que();

    // Set of table files to protect from deletion because they are part of ongoing compactions.
    Set<Long> pendingOutputs = new HashSet<>();

    // Has a background compaction been scheduled or is running?
    AtomicBoolean bgCompactionScheduled = new AtomicBoolean();

    // Have we encountered a background error in paranoid mode?
    Exception bgError; // Status bg_error_;

    Map<Snapshot,Long> snapshots = new LinkedHashMap<>();  // insert-order

    long snapshotsOldestNumber() {
        return snapshots.entrySet().iterator().next().getValue();
    }

    VersionSet versions;

    KeyComparator<Slice> userComparator() {
        return internalComparator.userComparator;
    }

    boolean ownsInfoLog;

    // Information kept for every waiting writer
    class Waiter { // struct DBImpl::Writer
        Exception status;
        Batch.Que batch;
        boolean sync;
        boolean done;
        Condition cv; // port::CondVar cv;
    }
    //   explicit Writer(port::Mutex* mu) : cv(mu) { }

    void open() {
        mutex.lock();
        try {
            VersionEdit edit = new VersionEdit();
            // Recover handles create_if_missing, error_if_exists
            Bool saveManifest = new Bool();
            recover(edit,saveManifest);
            if (memTable == null) {
                // Create new log and a corresponding memtable.
                long newLogNumber = versions.newFileNumber();
                OutputStream lfile = env.newWritableFile(logFileName(dbname,newLogNumber));
                edit.setLogNumber(newLogNumber);
                logfileNumber = newLogNumber;
                log = new LogWriter(lfile);
                memTable = new MemTable(internalComparator);
                memTable.ref();
            }
            if (saveManifest.v) {
                edit.setPrevLogNumber(0);
                edit.setLogNumber(logfileNumber);
                versions.logAndApply(edit,mutex);
            }
            deleteObsoleteFiles();
            maybeScheduleCompaction();
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }
        finally {
            mutex.unlock();
        }
        assert (memTable != null);
    }

    // DBImpl::~DBImpl()
    void close() throws IOException {
        if (shuttingDown.getAndSet(true)) {
            return; // exit if already shutting down
        }
        // Wait for background work to finish
        mutex.lock();
        try {
            while (bgCompactionScheduled.get()) {
                bgCv.awaitUninterruptibly(); // bg_cv_.Wait();
            }
        } finally {
            mutex.unlock();
        }

        if (dbLock != null) {
            env.unlockFile(dbLock);
        }

        versions.close(); // delete versions_;
        if (memTable != null) memTable.unref();
        if (immuTable != null) immuTable.unref();
        // delete tmp_batch_;
        log.close(); // delete log_;
        // delete logfile_;
        tableCache.close(); // delete table_cache_;

        if (ownsInfoLog) {
            Info.close(); // delete options_.info_log;
        }
        // if (owns_cache_) {
        //   delete options_.block_cache;
        // }
    }

    void newDB() throws IOException {
        VersionEdit new_db = new VersionEdit();
        new_db.setComparatorName(userComparator().name());
        new_db.setLogNumber(0);
        new_db.setNextFile(2);
        new_db.setLastSequence(0);

        Path manifest = descriptorFileName(dbname,1);
        OutputStream file = env.newWritableFile(manifest);
        LogWriter new_log = new LogWriter(file);
        byte[] record = new_db.encodeTo();
        new_log.addRecord(new Slice(record));
        new_log.close();

        // Make "CURRENT" file that points to the new manifest file.
        setCurrentFile(env,dbname,1);
    }

    /**
     * Recover the descriptor from persistent storage.
     * May do a significant amount of work to recover recently logged updates.
     * Any changes to be made to the descriptor are added to *edit.
     */
    void recover(VersionEdit edit, Bool saveManifest) throws IOException {
        assert (mutex.isHeldByCurrentThread());

        // Ignore error from CreateDir since the creation of the DB is
        // committed only when the descriptor is created, and this directory
        // may already exist from a previous failed creation attempt.
        env.createDir(dbname);
        assert (dbLock == null);
        dbLock = env.lockFile(lockFileName(dbname));

        if (!env.fileExists(currentFileName(dbname))) {
            if (createIfMissing) {
                newDB();
            } else {
                throw new Status(dbname.toString()+" does not exist (create_if_missing is false)")
                                .state(InvalidArgument);
            }
        } else {
            if (errorIfExists) {
                throw new Status(dbname.toString()+" exists (error_if_exists is true)")
                                .state(InvalidArgument);
            }
        }

        versions.recover(saveManifest);

        SequenceNumber maxSequence = new SequenceNumber();

        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).

        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.

        long minLog = versions.logNumber();
        long prevLog = versions.prevLogNumber();

        Path[] filenames = env.getChildren(dbname);

        Set<Long> expected = new HashSet<>();
        versions.addLiveFiles(expected);

        List<Long> logs = new ArrayList<>();
        for (int i = 0; i < filenames.length; i++) {
            ParsedFileName p = parseFileName(filenames[i]);
            if (p == null) continue;
            expected.remove(p.number);
            if (p.type == FileType.kLogFile && ((p.number >= minLog) || (p.number == prevLog))) {
                logs.add(p.number);
            }
        }
        if (!expected.isEmpty()) {
            throw new Status("missing log files: "+Arrays.toString(expected.toArray()))
                             .state(Corruption);
            // char buf[50];
            // snprintf(buf, sizeof(buf), "%d missing files; e.g.", static_cast<int>(expected.size()));
            // return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
        }

        // Recover in the order in which the logs were generated
        logs.sort(null);
        for (int i = 0; i < logs.size(); i++) {
            recoverLogFile(logs.get(i), (i == logs.size() - 1), saveManifest, edit,
                           maxSequence);

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            versions.markFileNumberUsed(logs.get(i));
        }

        if (versions.lastSequence() < maxSequence.v) {
            versions.setLastSequence(maxSequence.v);
        }
    }

//  struct LogReporter : public log::Reader::Reporter {
//    Env* env;
//    Logger* info_log;
//    const char* fname;
//    Status* status;  // NULL if options_.paranoid_checks==false
//    virtual void Corruption(size_t bytes, const Status& s) {
//      Log(info_log, "%s%s: dropping %d bytes; %s",
//          (this->status == NULL ? "(ignoring error) " : ""),
//          fname, static_cast<int>(bytes), s.ToString().c_str());
//      if (this->status != NULL && this->status->ok()) *this->status = s;
//    }
//  };

//  LogReporter reporter;
//  reporter.env = env_;
//  reporter.info_log = options_.info_log;
//  reporter.fname = fname.c_str();
//  reporter.status = (options_.paranoid_checks ? &status : NULL);
//  log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);

    void recoverLogFile(long logNumber, boolean lastLog,
        Bool saveManifest, VersionEdit edit,
        SequenceNumber maxSequence) throws IOException
    {
        assert (mutex.isHeldByCurrentThread());

        // Open the log file
        Path fname = logFileName(dbname, logNumber);
        InputStream file = env.newSequentialFile(fname);
        // if (!status.ok()) {
        //   MaybeIgnoreError(&status);
        //   return status;
        // }

        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).

        // Create the log reader.
        LogReader reader = new LogReader(file);
            // log::Reader reader(file, &reporter, true/*checksum*/,0/*initial_offset*/);
            // TODO: use &reporter in log file recovery
        Info.log("Recovering log #%d", logNumber );

        // Read all the records and add to a memtable
        Batch.Rep batch = new Batch.Rep();
        int compactions = 0;
        MemTable mem = null;

        for (Slice record : reader) {
            if (record.length < 12) {
                throw new Status("log record too small").state(Corruption);
                // reporter.Corruption( record.size(), Status::Corruption("log record too small"));
            }
            batch.setContents(record);

            if (mem == null) {
                mem = new MemTable(internalComparator);
                mem.ref();
            }
            insertInto(batch,mem);
            // MaybeIgnoreError(&status);

            long lastSeq = batch.sequence() + batch.count() - 1;
            if (lastSeq > maxSequence.v) {
                maxSequence.v = lastSeq;
            }

            if (mem.approximateMemoryUsage() > writeBufferSize) {
                compactions++;
                saveManifest.v = true;
                writeLevel0Table(mem, edit, null);
                mem.unref();
                mem = null;
                // if (!status.ok()) {
                //   // Reflect errors immediately so that conditions like full
                //   // file-systems cause the DB::Open() to fail.
                //   break;
                // }
            }
        } // for(record)

        // delete file;

        // See if we should keep reusing the last log file.
        if (reuseLogs && lastLog && compactions == 0) {
            // assert(logfile == null);
            assert (log == null);
            assert (memTable == null);

            if (env.getFileSize(fname) > 0) {
                OutputStream lfile = env.newAppendableFile(fname);
                Info.log("Reusing old log %s", fname.toString() );
                log = new LogWriter(lfile); // log_ = new log::Writer(logfile_, lfile_size);
                logfileNumber = logNumber;
                if (mem != null) {
                    memTable = mem;
                    mem = null;
                } else {
                    // mem can be NULL if lognum exists but was empty.
                    memTable = new MemTable(internalComparator);
                    memTable.ref();
                }
            }
        }

        if (mem != null) {
            // mem did not get reused; compact it.
            saveManifest.v = true;
            writeLevel0Table(mem, edit, null);
            mem.unref();
        }
    }

    // WriteBatchInternal::InsertInto(WriteBatch &batch, MemTable mem)
    static long insertInto(Batch.Write batch, MemTable mem) {
        long sequence = batch.sequence();
        for (Entry<Slice,Slice> e : batch) {
            Slice value = e.getValue();
            int type = (value != null) ? kTypeValue : kTypeDeletion ;
            mem.add(sequence,type,e.getKey(),value);
            sequence++;
        }
        return sequence;
    }

    void writeLevel0Table(MemTable mem, VersionEdit edit, Version base) { // throws IOException {
        assert (mutex.isHeldByCurrentThread());

        final long startMicros = env.nowMicros();
        FileMetaData meta;
        long fileNumber = versions.newFileNumber();
        pendingOutputs.add(fileNumber);
        Cursor<InternalKey,Slice> iter = mem.newIterator();
        Info.log("Level-0 table #%d: started", fileNumber );

        mutex.unlock();
        try {
            meta = Table.store(dbname, env, fileNumber,
                internalComparator, filterPolicy,
                blockSize, blockRestartInterval, compression,
                iter, tableCache );
            // BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
        }
        finally {
            mutex.lock();
        }

        Info.log("Level-0 table #%d: %d bytes", meta.number, meta.fileSize );
        pendingOutputs.remove(meta.number);

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        int level = 0;
        if (meta.fileSize > 0) {
            if (base != null) {
                Slice minUserKey = meta.smallest.userKey;
                Slice maxUserKey = meta.largest.userKey;
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }
            edit.addFile(level, meta.number, meta.fileSize, meta.smallest, meta.largest );
        }

        addCompactionStats(level, env.nowMicros() - startMicros, 0, meta.fileSize );
    }

    // Delete any unneeded files and stale in-memory entries.
    void deleteObsoleteFiles() {
        if (bgError != null) {
            // After a background error, we don't know whether a new version may
            // or may not have been committed, so we cannot safely garbage collect.
            return;
        }

        // Make a set of all of the live files
        Set<Long> live = new HashSet<>(pendingOutputs);
        versions.addLiveFiles(live);

        Path[] filenames = env.getChildren(dbname); // Ignoring errors on purpose

        for (int i = 0; i < filenames.length; i++) {
            ParsedFileName p = parseFileName(filenames[i]);
            if (p == null) continue; // if (ParseFileName(filenames[i], &number, &type)) {
            boolean keep = true;
            long number = p.number;
            FileType type = p.type;
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
                if (type == FileType.kTableFile) {
                    tableCache.evict(number);
                }
                Info.log("Delete %s #%d",
                         type.name(), // int(type),
                         number);
                try {
                    env.deleteFile(filenames[i]);
                }
                catch (IOException e) {
                    throw new Status(e).state(IOError);
                }
            }
        }
    }

    // Implementations of the DB interface

    void put(WriteOptions options, Slice key, Slice value) {
        WriteBatch batch = new Batch.Que();
        batch.put(key,value);
        write(options,batch);
    }

    void delete(WriteOptions options, Slice key) {
        WriteBatch batch = new Batch.Que();
        batch.delete(key);
        write(options,batch);
    }

    void write(WriteOptions options, WriteBatch myBatch) {
        Waiter w = new Waiter();
        w.batch = (Batch.Que) myBatch;
        w.sync = options.sync;
        w.done = false;
        w.cv = mutex.newCondition();

        try (MutexLock l = mutex.open()) // MutexLock l(&mutex_);
        {
            writers.add(w);
            while ( !w.done && w != writers.peek() ) {
                w.cv.awaitUninterruptibly();
            }
            if (w.done) {
                if (w.status != null) throw Status.check(w.status);
            }

            // May temporarily unlock and wait.
            Exception status = null;
            makeRoomForWrite(myBatch == null);
            long lastSequence = versions.lastSequence();
            Ref<Waiter> lastWriter = new Ref<>(w); // Writer* last_writer = &w;
            if (myBatch != null) { // NULL batch is for compactions
                Batch.Que updates = buildBatchGroup(lastWriter);
                updates.setSequence(lastSequence + 1);
                lastSequence += updates.count();

                // Add to log and apply to memtable.  We can release the lock
                // during this phase since &w is currently responsible for logging
                // and protects against concurrent loggers and concurrent writes into mem_.

                mutex.unlock();
                try {
                    log.addRecord(updates.contents());
                    if (options.sync) {
                        env.syncFile(log.out); // status = logfile_->Sync();
                    }
                    insertInto(updates,memTable);
                }
                catch (Exception e) {
                    status = e;
                }
                mutex.lock();
                if (status != null) { // if (sync_error)
                    // The state of the log file is indeterminate: the log record we
                    // just added may or may not show up when the DB is re-opened.
                    // So we force the DB into a mode where all future writes fail.
                    recordBackgroundError(status); // RecordBackgroundError(status);
                }
                if (updates == tmpBatch) tmpBatch.clear();

                versions.setLastSequence(lastSequence);
            }

            while (true) {
                Waiter ready = writers.peek();
                writers.poll();
                if (ready != w) {
                    ready.status = status;
                    ready.done = true;
                    ready.cv.signal();
                }
                if (ready == lastWriter.v) break;
            }

            // Notify new head of write queue
            if (!writers.isEmpty()) {
                writers.peek().cv.signal();
            }
        }
    }

    // REQUIRES: mutex_ is held
    // REQUIRES: this thread is currently at the front of the writer queue
    void makeRoomForWrite(boolean force) {
        assert (mutex.isHeldByCurrentThread());
        assert (!writers.isEmpty());
        boolean allowDelay = !force;
        // Status s;
        while (true) {
            if (bgError != null) {
                // Yield previous error
                throw Status.check(bgError); //s = bg_error_; break;
            } else if (
                allowDelay &&
                versions.numLevelFiles(0) >= kL0_SlowdownWritesTrigger) {
                // We are getting close to hitting a hard limit on the number of L0 files.
                // Rather than delaying a single write by several seconds when we hit the hard limit,
                // start delaying each individual write by 1ms to reduce latency variance.
                // Also, this delay hands over some CPU to the compaction thread
                // in case it is sharing the same core as the writer.
                mutex.unlock();
                env.sleepForMicroseconds(1000); // env_->SleepForMicroseconds(1000);
                allowDelay = false;  // Do not delay a single write more than once
                mutex.lock();
            } else if (!force &&
                       (memTable.approximateMemoryUsage() <= writeBufferSize)) {
                // There is room in current memtable
                break;
            } else if (immuTable != null) {
                // We have filled up the current memtable, but the previous
                // one is still being compacted, so we wait.
                Info.log("Current memtable full; waiting...");
                bgCv.awaitUninterruptibly();
            } else if (versions.numLevelFiles(0) >= kL0_StopWritesTrigger) {
                // There are too many level-0 files.
                Info.log("Too many L0 files; waiting...");
                bgCv.awaitUninterruptibly();
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                assert (versions.prevLogNumber() == 0);
                long newLogNumber = versions.newFileNumber();
                try {
                    Path lpath = logFileName(dbname,newLogNumber);
                    OutputStream lfile = env.newWritableFile(lpath);
                    LogWriter newLog = new LogWriter(lfile);
                    log.close(); // delete log_;
                    logfileNumber = newLogNumber;
                    log = newLog;
                }
                catch (IOException e) { // if (!s.ok()) {
                    // Avoid chewing through file number space in a tight loop.
                    versions.reuseFileNumber(newLogNumber);
                    throw new Status(e).state(IOError); // break;
                }
                immuTable = memTable; // imm_ = mem_;
                has_imm.set(immuTable); // has_imm_.Release_Store(immu_table_);
                memTable = new MemTable(internalComparator);
                memTable.ref();
                force = false;   // Do not force another compaction if have room
                maybeScheduleCompaction();
            }
        }
    }

    // REQUIRES: Writer list must be non-empty
    // REQUIRES: First writer must have a non-NULL batch
    Batch.Que buildBatchGroup(Ref<Waiter> lastWriter) {
        assert (!writers.isEmpty());
        Waiter first = writers.peek();
        Batch.Que result = first.batch;
        assert (result != null);

        int size = first.batch.byteSize();

        // Allow the group to grow up to a maximum size,
        // but if the original write is small, limit the growth
        // so we do not slow down the small write too much.
        int max_size = 1 << 20;
        if (size <= (128<<10)) {
            max_size = size + (128<<10);
        }

        lastWriter.v = first;
        Iterator<Waiter> iter = writers.iterator();
        iter.next(); // Advance past "first"
        while (iter.hasNext()) {
            Waiter w = iter.next();
            if (w.sync && !first.sync) {
                //Do not include a sync write into a batch handled by a non-sync write.
                break;
            }

            if (w.batch != null) {
                size += w.batch.byteSize();
                if (size > max_size) {
                    // Do not make batch too big
                    break;
                }

                // Append to *result
                if (result == first.batch) {
                    // Switch to temporary batch instead of disturbing caller's batch
                    result = tmpBatch;
                    assert (result.count() == 0);
                    result.append(first.batch);
                }
                result.append(w.batch);
            }
            lastWriter.v = w;
        }
        return result;
    }

    Slice get(ReadOptions options, Slice key) {
        Slice value = null;
        try (MutexLock l= mutex.open()) // MutexLock l(&mutex_);
        {
            long sequenceNumber;
            if (options.snapshot != null) {
                sequenceNumber = lookup(options.snapshot);
            } else {
                sequenceNumber = versions.lastSequence();
            }

            MemTable mem = memTable;
            MemTable imm = immuTable;
            Version current = versions.current();
            mem.ref();
            if (imm != null) imm.ref();
            current.ref();

            // boolean haveStatUpdate = false;
            Version.GetStats stats = null;

            // Unlock while reading from files and memtables
            mutex.unlock();
            // LookupKey lkey(key, snapshot);
            try {
                // First look in the memtable, then in the immutable memtable (if any).
                if ((value = mem.get(sequenceNumber, key)) != null) { // mem->Get(lkey, value, &s))
                    // Done
                } else if (imm != null && (value = imm.get(sequenceNumber, key)) != null) { // imm->Get(lkey, value, &s)
                    // Done
                } else {
                    // s = current->Get(options, lkey, value, &stats);
                    // stats = current.getStats();
                    // haveStatUpdate = true;
                    stats = current.get(key,sequenceNumber,options.verifyChecksums,options.fillCache);
                    value = stats.value;
                    // TODO: check stats and set value as appropriate
                }
            }
            finally {
                mutex.lock();
            }

            if (stats != null && current.updateStats(stats)) {
                maybeScheduleCompaction();
            }
            mem.unref();
            if (imm != null) imm.unref();
            current.unref();
        }
        return value;
    }

    Snapshot getSnapshot() {
        try (MutexLock l = mutex.open()) { // MutexLock l(&mutex_);
            Snapshot key = new Snapshot(){};
            Long value = versions.lastSequence();
            snapshots.put(key,value);
            return key;
        }
    }

    void releaseSnapshot(Snapshot key) {
        try (MutexLock l = mutex.open()) { // MutexLock l(&mutex_);
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

    void recordBackgroundError(Exception s) {
        assert (mutex.isHeldByCurrentThread());
        if (bgError == null) {
            bgError = s;
            bgCv.signalAll();
        }
        System.err.println("background error");
        s.printStackTrace();
    }

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod bytes.
    void recordReadSample(InternalKey key) {
        try (MutexLock l = mutex.open()) {  // MutexLock l(&mutex_);
            if (versions.current().recordReadSample(key)) {
                maybeScheduleCompaction();
            }
        }
    }

    // TODO: // void DBImpl::MaybeIgnoreError(Status* s) const {
    //   if (s->ok() || options_.paranoid_checks) {
    //     // No change needed
    //   } else {
    //     Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    //     *s = Status::OK();
    //   }
    // }

    // no-op here
    void addCompactionStats(int level, long timeUsed, long bytesRead, long bytesWritten) {}

    // no-op here
    void maybeScheduleCompaction() {}

}


/*
[root@m4 leveldb]# grep -rn 'MaybeSchedule' *
db/db_impl.cc:610:      MaybeScheduleCompaction();
db/db_impl.cc:645:void DBImpl::MaybeScheduleCompaction() {
db/db_impl.cc:682:  MaybeScheduleCompaction();
db/db_impl.cc:1148:    MaybeScheduleCompaction();
db/db_impl.cc:1171:    MaybeScheduleCompaction();
db/db_impl.cc:1375:      MaybeScheduleCompaction();
db/db_impl.cc:1522:    impl->MaybeScheduleCompaction();
db/db_impl.h:107:  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
[root@m4 leveldb]#

*/