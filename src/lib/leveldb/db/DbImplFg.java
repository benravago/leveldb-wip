package lib.leveldb.db;

import java.io.IOException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.HashSet;

import java.util.concurrent.locks.Condition;

import lib.leveldb.Env;
import lib.leveldb.Slice;
import static lib.leveldb.DB.*;
import static lib.leveldb.db.DbUtil.*;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.FileName.*;

class DbImplFg extends DbImpl {

    // Queue of writers.
    Deque<Waiter> writers = new LinkedList<>(); // std::deque<Writer*> writers_;
    Batch.Write tmpBatch; //  = new Batch.Que();

    // Information kept for every waiting writer
    class Waiter { // struct DBImpl::Writer
        Exception status;
        Batch.Write batch;
        boolean sync;
        boolean done;
        Condition cv; // port::CondVar cv;
    }
    //   explicit Writer(port::Mutex* mu) : cv(mu) { }

    void open() {
        mutex.lock();
        try (mutex) {
            var edit = new VersionEdit();
            tmpBatch = batch();
            // Recover handles create_if_missing, error_if_exists
            var saveManifest = new Bool();
            recover(edit,saveManifest);
            if (memTable == null) {
                // Create new log and a corresponding memtable.
                var newLogNumber = versions.newFileNumber();
                var lfile = env.newWritableFile(logFileName(dbname,newLogNumber));
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
            throw ioerror(e);
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
        try (mutex) {
            while (bgCompactionScheduled.get()) {
                bgCv.awaitUninterruptibly(); // bg_cv_.Wait();
            }
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

//      if (ownsInfoLog) {
//          Info.close(); // delete options_.info_log;
//      }
        // if (owns_cache_) {
        //   delete options_.block_cache;
        // }
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
                throw invalidArgument(dbname.toString()+" does not exist (create_if_missing is false)");
            }
        } else {
            if (errorIfExists) {
                throw invalidArgument(dbname.toString()+" exists (error_if_exists is true)");
            }
        }

        versions.recover(saveManifest);

        var maxSequence = new SequenceNumber();

        // Recover from all newer log files than the ones named in the
        // descriptor (new log files may have been added by the previous
        // incarnation without registering them in the descriptor).

        // Note that PrevLogNumber() is no longer used, but we pay
        // attention to it in case we are recovering a database
        // produced by an older version of leveldb.

        var minLog = versions.logNumber();
        var prevLog = versions.prevLogNumber();

        var filenames = env.getChildren(dbname);

        var expected = new HashSet<Long>();
        versions.addLiveFiles(expected);

        var logs = new ArrayList<Long>();
        for (var filename : filenames) {
            var p = parseFileName(filename);
            if (p == null) continue;
            expected.remove(p.number);
            if (p.type == FileType.kLogFile && ((p.number >= minLog) || (p.number == prevLog))) {
                logs.add(p.number);
            }
        }
        if (!expected.isEmpty()) {
            throw corruption("missing log files: "+Arrays.toString(expected.toArray()));
            // char buf[50];
            // snprintf(buf, sizeof(buf), "%d missing files; e.g.", static_cast<int>(expected.size()));
            // return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
        }

        // Recover in the order in which the logs were generated
        logs.sort(null);
        var lastLog = logs.size() - 1;
        for (var i = 0; i < logs.size(); i++) {
            var logNumber = logs.get(i);
            recoverLogFile( logNumber, (i == lastLog),
                            saveManifest, edit, maxSequence );

            // The previous incarnation may not have written any MANIFEST
            // records after allocating this log number.  So we manually
            // update the file number allocation counter in VersionSet.
            versions.markFileNumberUsed(logNumber);
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
        var fname = logFileName(dbname, logNumber);
        var file = env.newSequentialFile(fname);
        // if (!status.ok()) {
        //   MaybeIgnoreError(&status);
        //   return status;
        // }

        // We intentionally make log::Reader do checksumming even if
        // paranoid_checks==false so that corruptions cause entire commits
        // to be skipped instead of propagating bad information (like overly
        // large sequence numbers).

        // Create the log reader.
        var reader = new LogReader(file);
            // log::Reader reader(file, &reporter, true/*checksum*/,0/*initial_offset*/);
            // TODO: use &reporter in log file recovery
        info("Recovering log #{0,number}", logNumber );

        // Read all the records and add to a memtable
        var batch = new Batch.Read();
        var compactions = 0;
        MemTable mem = null;

        for (var record : reader) {
            if (record.length < 12) {
                throw corruption("log record too small");
                // reporter.Corruption( record.size(), Status::Corruption("log record too small"));
            }
            batch.setContents(record);

            if (mem == null) {
                mem = new MemTable(internalComparator);
                mem.ref();
            }
            insertInto(batch,mem);
            // MaybeIgnoreError(&status);

            var lastSeq = batch.sequence() + batch.count() - 1;
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
                var lfile = env.newAppendableFile(fname);
                info("Reusing old log {0}", fname.toString() );
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
    static long insertInto(Batch.Rep batch, MemTable mem) {
        var sequence = batch.sequence();
        for (var e : batch) {
            var value = e.getValue();
            var type = (value != null) ? kTypeValue : kTypeDeletion ;
            mem.add(sequence,type,e.getKey(),value);
            sequence++;
        }
        return sequence;
    }

    void writeLevel0Table(MemTable mem, VersionEdit edit, Version base) { // throws IOException {
        assert (mutex.isHeldByCurrentThread());

        final long startMicros = env.nowMicros();
        FileMetaData meta;
        var fileNumber = versions.newFileNumber();
        pendingOutputs.add(fileNumber);
        var iter = mem.newIterator();
        info("Level-0 table #{0,number}: started", fileNumber );

        mutex.unlock();
        try (mutex) {
            meta = Table.store(dbname,
                fileNumber, blockSize, blockRestartInterval,
                env, internalComparator, filterPolicy,
                iter, compression );
            // BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
        }

        info("Level-0 table #{0,number}: {1,number} bytes", meta.number, meta.fileSize );
        pendingOutputs.remove(meta.number);

        // Note that if file_size is zero, the file has been deleted and
        // should not be added to the manifest.
        var level = 0;
        if (meta.fileSize > 0) {
            if (base != null) {
                var minUserKey = meta.smallest.userKey;
                var maxUserKey = meta.largest.userKey;
                level = base.pickLevelForMemTableOutput(minUserKey, maxUserKey);
            }
            edit.addFile(level, meta.number, meta.fileSize, meta.smallest, meta.largest );
        }

        addCompactionStats(level, env.nowMicros() - startMicros, 0, meta.fileSize );
    }


//  // Implementations of the DB interface

//  void put(WriteOptions options, Slice key, Slice value) {
//      WriteBatch batch = new Batch.Que();
//      batch.put(key,value);
//      write(options,batch);
//  }
//
//  void delete(WriteOptions options, Slice key) {
//      WriteBatch batch = new Batch.Que();
//      batch.delete(key);
//      write(options,batch);
//  }

    Batch.Write batch() {
        return new Batch.Write() {
            @Override public void apply(boolean sync) {} // write(this,sync);
        };
    }

    // void write(WriteOptions options, WriteBatch myBatch) {
    void write(WriteBatch myBatch, boolean sync) {
        var w = new Waiter();
        w.batch = (Batch.Write) myBatch;
        w.sync = sync;
        w.done = false;
        w.cv = mutex.newCondition();

        mutex.lock();
        try (mutex) // MutexLock l(&mutex_);
        {
            writers.add(w);
            while ( !w.done && w != writers.peek() ) {
                w.cv.awaitUninterruptibly();
            }
            if (w.done) {
                if (w.status != null) throw check(w.status);
            }

            // May temporarily unlock and wait.
            Exception status = null;
            makeRoomForWrite(myBatch == null);
            var lastSequence = versions.lastSequence();
            var lastWriter = new Ref<>(w); // Writer* last_writer = &w;
            if (myBatch != null) { // NULL batch is for compactions
                var updates = buildBatchGroup(lastWriter);
                updates.setSequence(lastSequence + 1);
                lastSequence += updates.count();

                // Add to log and apply to memtable.  We can release the lock
                // during this phase since &w is currently responsible for logging
                // and protects against concurrent loggers and concurrent writes into mem_.

                mutex.unlock();
                try {
                    log.addRecord(updates.contents());
                    if (sync) {
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
                var ready = writers.peek();
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
        var allowDelay = !force;
        // Status s;
        while (true) {
            if (bgError != null) {
                // Yield previous error
                throw check(bgError); //s = bg_error_; break;
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
                info("Current memtable full; waiting...");
                bgCv.awaitUninterruptibly();
            } else if (versions.numLevelFiles(0) >= kL0_StopWritesTrigger) {
                // There are too many level-0 files.
                info("Too many L0 files; waiting...");
                bgCv.awaitUninterruptibly();
            } else {
                // Attempt to switch to a new memtable and trigger compaction of old
                assert (versions.prevLogNumber() == 0);
                var newLogNumber = versions.newFileNumber();
                try {
                    var lpath = logFileName(dbname,newLogNumber);
                    var lfile = env.newWritableFile(lpath);
                    var newLog = new LogWriter(lfile);
                    log.close(); // delete log_;
                    logfileNumber = newLogNumber;
                    log = newLog;
                }
                catch (IOException e) { // if (!s.ok()) {
                    // Avoid chewing through file number space in a tight loop.
                    versions.reuseFileNumber(newLogNumber);
                    throw ioerror(e); // break;
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
    Batch.Write buildBatchGroup(Ref<Waiter> lastWriter) {
        assert (!writers.isEmpty());
        var first = writers.peek();
        var result = first.batch;
        assert (result != null);

        var size = first.batch.byteSize();

        // Allow the group to grow up to a maximum size,
        // but if the original write is small, limit the growth
        // so we do not slow down the small write too much.
        var max_size = 1 << 20;
        if (size <= (128<<10)) {
            max_size = size + (128<<10);
        }

        lastWriter.v = first;
        var iter = writers.iterator();
        iter.next(); // Advance past "first"
        while (iter.hasNext()) {
            var w = iter.next();
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

    // Slice get(ReadOptions options, Slice key) {
    Slice get(Slice key, Snapshot snapshot, boolean fillCache, boolean verifyChecksums) {
        Slice value = null;
        mutex.lock();
        try (mutex) // MutexLock l(&mutex_);
        {
            long sequenceNumber;
            if (snapshot != null) {
                sequenceNumber = lookup(snapshot);
            } else {
                sequenceNumber = versions.lastSequence();
            }

            var mem = memTable;
            var imm = immuTable;
            var current = versions.current();
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
                    stats = current.get(key,sequenceNumber,verifyChecksums,fillCache);
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


    void recordBackgroundError(Exception s) {
        assert (mutex.isHeldByCurrentThread());
        if (bgError == null) {
            bgError = s;
            bgCv.signalAll();
        }
        infoLog.error(s,"background error");
    }

    // Record a sample of bytes read at the specified internal key.
    // Samples are taken approximately once every config::kReadBytesPeriod bytes.
    @Override
    void recordReadSample(InternalKey key) {
        mutex.lock();
        try (mutex) {  // MutexLock l(&mutex_);
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
