package bsd.leveldb.db;

import java.nio.file.Path;
import java.io.IOException;
import java.io.OutputStream;

import java.util.List;
import java.util.ArrayList;

import bsd.leveldb.Slice;
import bsd.leveldb.Status;
import bsd.leveldb.Cursor;
import bsd.leveldb.WriteOptions;
import bsd.leveldb.io.Info;
import bsd.leveldb.io.MutexLock;
import bsd.leveldb.db.Versions.Compaction;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.FileName.*;
import static bsd.leveldb.Status.Code.*;
import static bsd.leveldb.db.Struct.*;

class DbImpl extends DbMain {

    // virtual void CompactRange(const Slice* begin, const Slice* end);
    void compactRange(Slice begin, Slice end) {
        int maxLevelWithFiles = 1;
        try (MutexLock l = mutex.open()) // MutexLock l(&mutex_);
        {
            Version base = versions.current();
            for (int level = 1; level < kNumLevels; level++) {
                if (base.overlapInLevel(level, begin, end)) {
                    maxLevelWithFiles = level;
                }
            }
        }
        xCompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
        for (int level = 0; level < maxLevelWithFiles; level++) {
            xCompactRange(level, begin, end);
        }
    }

    // Extra methods (visible for testing) that are not in the public DB interface

    // Force current memtable contents to be compacted.
    void xCompactMemTable() {
        // NULL batch means just wait for earlier writes to be done
        write(new WriteOptions(), null);
        // Wait until the compaction completes
        try (MutexLock l = mutex.open())  // MutexLock l(&mutex_);
        {
            while (immuTable != null && bgError == null) {
                bgCv.awaitUninterruptibly();
            }
            if (immuTable != null) {
                throw new Status(bgError);
            }
        }
    }

    // Compact any files in the named level that overlap [*begin,*end]
    void xCompactRange(int level, Slice begin, Slice end) {
        assert (level >= 0);
        assert (level + 1 < kNumLevels);

        InternalKey beginStorage, endStorage;

        ManualCompaction manual = new ManualCompaction();
        manual.level = level;
        manual.done = false;
        if (begin == null) {
            manual.begin = null;
        } else {
            beginStorage = internalKey(begin, kMaxSequenceNumber, kValueTypeForSeek);
            manual.begin = beginStorage;
        }
        if (end == null) {
            manual.end = null;
        } else {
            endStorage = internalKey(end, 0, 0);
            manual.end = endStorage;
        }

        try (MutexLock l = mutex.open())
        {
            while (!manual.done && !shuttingDown.get() && bgError == null) {
                if (manualCompaction == null) { // Idle
                    manualCompaction = manual;
                    maybeScheduleCompaction();
                } else {  // Running either my compaction or another compaction.
                    bgCv.awaitUninterruptibly();
                }
            }
            if (manualCompaction == manual) {
                // Cancel my manual compaction since we aborted early for some reason.
                manualCompaction = null;
            }
        }
    }


    // Information for a manual compaction
    class ManualCompaction {
        int level;
        boolean done;
        InternalKey begin;       // NULL means beginning of key range
        InternalKey end;         // NULL means end of key range
        InternalKey tmpStorage;  // Used to keep track of compaction progress
    }
    ManualCompaction manualCompaction = null;

    // Per level compaction stats.
    // stats_[level] stores the stats for compactions that produced data for the specified "level".
    class CompactionStats {
        long micros;
        long bytesRead;
        long bytesWritten;
    }
    CompactionStats[] stats = repeat(CompactionStats::new, kNumLevels); // newCompactionStats(kNumLevels);

    @Override
    void addCompactionStats(int level, long micros, long bytesRead, long bytesWritten) {
        CompactionStats s = stats[level];
        s.micros += micros;
        s.bytesRead += bytesRead;
        s.bytesWritten += bytesWritten;
    }

    @Override
    void maybeScheduleCompaction() {
        assert (mutex.isHeldByCurrentThread());
        if (bgCompactionScheduled.get()) {
            // Already scheduled
        } else if (shuttingDown.get()) {
            // DB is being deleted; no more background compactions
        } else if (bgError != null) {
            // Already got an error; no more changes
        } else if (immuTable == null &&
                   manualCompaction == null &&
                   !versions.needsCompaction()) {
            // No work to be done
        } else {
            bgCompactionScheduled.set(true); // bg_compaction_scheduled_ = true;
            env.schedule(BGWork); // env_->Schedule(&DBImpl::BGWork, this);
        }
    }

    // void DBImpl::BGWork(void* db) {
    //   reinterpret_cast<DBImpl*>(db)->BackgroundCall();
    // }
    Runnable BGWork = this::backgroundCall;

    void backgroundCall() {
        try (MutexLock l = mutex.open())
        {
            assert (bgCompactionScheduled.get());
            if (shuttingDown.get()) {
                // No more background work when shutting down.
            } else if (bgError != null) {
                // No more background work after a background error.
            } else {
                backgroundCompaction();
            }

            bgCompactionScheduled.set(false);

            // Previous compaction may have produced too many files in a level,
            // so reschedule another compaction if needed.
            maybeScheduleCompaction();
            bgCv.signalAll();
        }
        catch (Exception e) {
            recordBackgroundError(e);
        }
    }

    void backgroundCompaction() {
        assert (mutex.isHeldByCurrentThread());

        if (immuTable != null) {
            compactMemTable();
            return;
        }

        Compaction c;
        boolean isManual = (manualCompaction != null);
        InternalKey manualEnd = null;
        if (isManual) {
            ManualCompaction m = manualCompaction;
            c = versions.compactRange(m.level, m.begin, m.end);
            m.done = (c == null);
            if (c != null) {
                manualEnd = c.input(0, c.numInputFiles(0) - 1).largest;
            }
            Info.log("Manual compaction at level-%d from %s .. %s; will stop at %s",
                     m.level,
                    (m.begin != null ? Debug.string(m.begin) : "(begin)"),
                    (m.end != null ? Debug.string(m.end) : "(end)"),
                    (m.done ? "(end)" : Debug.string(manualEnd)));
        } else {
            c = Versions.pickCompaction(versions);
        }

        if (c == null) {
            // Nothing to do
        } else if (!isManual && c.isTrivialMove()) {
            // Move file to next level
            assert (c.numInputFiles(0) == 1);
            FileMetaData f = c.input(0, 0);
            c.edit().deleteFile(c.level(), f.number);
            c.edit().addFile(c.level() + 1, f.number, f.fileSize,
                             f.smallest, f.largest);
            versions.logAndApply(c.edit(),mutex);
            // if (!status.ok()) {
            //   RecordBackgroundError(status);
            // }
            Info.log("Moved #%d to level-%d %d bytes %s: %s",
                     f.number,
                     c.level() + 1,
                     f.fileSize,
                     "ok", // status.ToString().c_str(),
                     versions.levelSummary());
        } else {
            CompactionState compact = new CompactionState();
            compact.compaction = c;
            compact.doCompactionWork();
            // if (!status.ok()) {
            //   RecordBackgroundError(status);
            // }
            compact.cleanupCompaction();
            // delete compact;
            c.releaseInputs();
            deleteObsoleteFiles();
        }
        // delete c;

        // if (status.ok()) {
        //   // Done
        // } else if (shutting_down_.Acquire_Load()) {
        //   // Ignore compaction errors found during shutting down
        // } else {
        //   Log(options_.info_log,
        //       "Compaction error: %s", status.ToString().c_str());
        // }

        if (isManual) {
            ManualCompaction m = manualCompaction;
            // if (!status.ok()) {
            //   m->done = true;
            // }
            if (!m.done) {
               // We only compacted part of the requested range.
               // Update *m to the range that is left to be compacted.
               m.tmpStorage = manualEnd;
               m.begin = m.tmpStorage;
            }
            manualCompaction = null;
        }
    }

    /**
     * Compact the in-memory write buffer to disk.
     * Switches to a new log-file/memtable and writes a new descriptor iff successful.
     * Errors are recorded in bg_error_.
     * // EXCLUSIVE_LOCKS_REQUIRED(mutex_);
     */
    void compactMemTable() {
        assert (mutex.isHeldByCurrentThread());
        assert (immuTable != null);

        // Save the contents of the memtable as a new Table
        VersionEdit edit = new VersionEdit();
        Version base = versions.current();
        base.ref();
        writeLevel0Table(immuTable, edit, base);
        base.unref();

        if (shuttingDown.get()) {
            throw new Status("Deleting DB during memtable compaction");
        }

        // Replace immutable memtable with the generated Table
        edit.setPrevLogNumber(0);
        edit.setLogNumber(logfileNumber);  // Earlier logs no longer needed
        versions.logAndApply(edit, mutex);

        // Commit to the new state
        immuTable.unref();
        immuTable = null;
        has_imm.set(null);
        deleteObsoleteFiles();
    }

  class CompactionState {
    Compaction compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    long smallestSnapshot;

    // Files produced by compaction
    class Output {
        long number;
        long fileSize;
        InternalKey smallest, largest;
    }
    List<Output> outputs = new ArrayList<>();

    // State kept for output being generated
    OutputStream outfile;
    TableBuilder builder;

    long totalBytes = 0;

    Output currentOutput() {
        return outputs.isEmpty() ? null : outputs.get(outputs.size()-1);
    }

    // Status DBImpl::DoCompactionWork(CompactionState* compact)
    void doCompactionWork() {
        long startMicros = env.nowMicros();
        long immMicros = 0;  // Micros spent doing imm_ compactions

        Info.log("Compacting %d@%d + %d@%d files",
                 compaction.numInputFiles(0),
                 compaction.level(),
                 compaction.numInputFiles(1),
                 compaction.level() + 1);

        assert (versions.numLevelFiles(compaction.level()) > 0);
        assert (builder == null);
        assert (outfile == null);
        if (snapshots.isEmpty()) {
            smallestSnapshot = versions.lastSequence();
        } else {
            smallestSnapshot = snapshotsOldestNumber();
        }

        // Release mutex while we're actually doing the compaction work
        mutex.unlock();

        Cursor<InternalKey,Slice> input =  // Iterator* input = versions_->MakeInputIterator(compact->compaction);
            compaction.makeInputIterator(versions); // input->SeekToFirst();

        // ParsedInternalKey ikey;
        Slice currentUserKey = null;
        boolean hasCurrentUserKey = false;
        long lastSequenceForKey = kMaxSequenceNumber;
        while (input.hasNext() && !shuttingDown.get()) { // for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
            input.next();

            // Prioritize immutable compaction work
            if (has_imm.get() != null) { // has_imm_.NoBarrier_Load() != NULL) {
                long immStart = env.nowMicros();
                mutex.lock();
                if (immuTable != null) {
                    compactMemTable();
                    bgCv.signalAll();  // Wakeup MakeRoomForWrite() if necessary
                }
                mutex.unlock();
                immMicros += (env.nowMicros() - immStart);
            }

            InternalKey key = input.getKey();
            if (compaction.shouldStopBefore(key) &&
                builder != null)
            {
                finishCompactionOutputFile(); // status = FinishCompactionOutputFile(compact, input);
                // if (!status.ok()) {
                //   break;
                //  }
            }

            // Handle key/value, add to state, etc.
            boolean drop = false;
            if (validInternalKey(key)) { // if (!ParseInternalKey(key, &ikey)) {
                // Do not hide error keys
                currentUserKey = null; // current_user_key.clear();
                hasCurrentUserKey = false;
                lastSequenceForKey = kMaxSequenceNumber;
            } else {
                if (!hasCurrentUserKey ||
                    userComparator().compare(key.userKey,
                                             currentUserKey) != 0) {
                    // First occurrence of this user key
                    currentUserKey = key.userKey; // current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                    hasCurrentUserKey = true;
                    lastSequenceForKey = kMaxSequenceNumber;
                }

                if (lastSequenceForKey <= smallestSnapshot) {
                    // Hidden by an newer entry for same user key
                    drop = true;    // (A)
                } else if (valueType(key) == kTypeDeletion &&
                           sequenceNumber(key) <= smallestSnapshot &&
                           compaction.isBaseLevelForKey(key.userKey)) {
                    // For this user key:
                    // (1) there is no data in higher levels
                    // (2) data in lower levels will have larger sequence numbers
                    // (3) data in layers that are being compacted here and have
                    //     smaller sequence numbers will be dropped in the next
                    //     few iterations of this loop (by rule (A) above).
                    // Therefore this deletion marker is obsolete and can be dropped.
                    drop = true;
                }

                lastSequenceForKey = sequenceNumber(key);
            }

            // if 0 log(); below

            if (!drop) {
                // Open output file if necessary
                if (builder == null) {
                    openCompactionOutputFile();
                    // if (!status.ok()) {
                    //   break;
                    // }
                }
                if (builder.numEntries == 0) {
                    currentOutput().smallest = key;
                }
                currentOutput().largest = key;
                builder.add(key, input.getValue());

                // Close output file if it is big enough
                if (builder.fileSize() >=
                        compaction.maxOutputFileSize()) {
                    finishCompactionOutputFile(); // status = FinishCompactionOutputFile(compact, input);
                    // if (!status.ok()) {
                    //   break;
                    // }
                }
            }
        } // while(!shuttingDown)

        if (shuttingDown.get()) { //   if (status.ok() && shutting_down_.Acquire_Load()) {
            throw new Status("Deleting DB during compaction"); // status = Status::IOError("Deleting DB during compaction");
        }
        if (builder != null) { //   if (status.ok() && compact->builder != NULL) {
            finishCompactionOutputFile();
        }
        // if (status.ok()) {
        //   status = input->status();
        // }
        // delete input;
        // input = NULL;

        long bytesRead=0, bytesWritten=0;
        long timeUsed = env.nowMicros() - startMicros - immMicros;

        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < compaction.numInputFiles(which); i++) {
                bytesRead += compaction.input(which,i).fileSize;
            }
        }
        for (int i = 0; i < outputs.size(); i++) {
            bytesWritten += outputs.get(i).fileSize;
        }

        mutex.lock();
        addCompactionStats(compaction.level()+1, timeUsed, bytesRead, bytesWritten);

        installCompactionResults();
        // if (!status.ok()) {
        //   RecordBackgroundError(status);
        // }

        Info.log("compacted to: %s", versions.levelSummary());
    }

// #if 0
//     Log(options_.info_log,
//         "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
//         "%d smallest_snapshot: %d",
//         ikey.user_key.ToString().c_str(),
//         (int)ikey.sequence, ikey.type, kTypeValue, drop,
//         compact->compaction->IsBaseLevelForKey(ikey.user_key),
//         (int)last_sequence_for_key, (int)compact->smallest_snapshot);
// #endif

    void openCompactionOutputFile() {
        // assert (compact != NULL);
        assert (builder == null);
        long fileNumber;
        {
            mutex.lock();
            fileNumber = versions.newFileNumber();
            pendingOutputs.add(fileNumber);
            Output out = new Output();
            out.number = fileNumber;
            out.smallest = null;
            out.largest = null;
            outputs.add(out);
            mutex.unlock();
        }

        // Make the output file
        try {
            Path fname = tableFileName(dbname, fileNumber);
            outfile = env.newWritableFile(fname);
            builder = new TableBuilder(outfile,internalComparator);
        }
        catch (IOException e) { throw new Status(e).state(IOError); }
    }


    void finishCompactionOutputFile() {
        assert (outfile != null);
        assert (builder != null);

        long outputNumber = currentOutput().number;
        assert (outputNumber != 0);

        long currentEntries = builder.numEntries();
        long currentBytes = 0;
        try {
            builder.finish();

            currentBytes = builder.fileSize();
            currentOutput().fileSize = currentBytes;
            totalBytes += currentBytes;
            builder.close(); // delete compact->builder;
            builder = null;

            env.syncFile(outfile);
            outfile.close();
            // delete compact->outfile;
            outfile = null;
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }

        if (currentEntries > 0) {
            // Verify that the table is usable by trying to create an iterator
            boolean fillCache = false;
            tableCache.newIterator(outputNumber, currentBytes, fillCache);
            Info.log("Generated table #%d@%d: %d keys, %d bytes",
                     outputNumber,
                     compaction.level(),
                     currentEntries,
                     currentBytes);
        }
    }

    void installCompactionResults() {
        assert (mutex.isHeldByCurrentThread());
        Info.log("Compacted %d@%d + %d@%d files => %d bytes",
                 compaction.numInputFiles(0),
                 compaction.level(),
                 compaction.numInputFiles(1),
                 compaction.level() + 1,
                 totalBytes);

        // Add compaction outputs
        compaction.addInputDeletions(compaction.edit());
        int level = compaction.level();
        for (int i = 0; i < outputs.size(); i++) {
            Output out = outputs.get(i);
            compaction.edit().addFile(
                level + 1,
                out.number, out.fileSize, out.smallest, out.largest );
        }
        versions.logAndApply(compaction.edit(), mutex);
    }

    void cleanupCompaction() {
        assert (mutex.isHeldByCurrentThread());
        if (builder != null) {
            // May happen if we get a shutdown call in the middle of compaction
            builder.abandon();
            // delete compact->builder;
        } else {
            assert (outfile == null);
        }
        // delete compact->outfile;
        for (int i = 0; i < outputs.size(); i++) {
            Output out = outputs.get(i);
            pendingOutputs.remove(out.number);
        }
    }

  } // CompactionState

} // DbImpl