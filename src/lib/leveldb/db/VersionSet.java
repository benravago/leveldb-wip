package bsd.leveldb.db;

import java.nio.file.Path;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import java.util.concurrent.locks.ReentrantLock;

import bsd.leveldb.Slice;
import bsd.leveldb.Status;
import bsd.leveldb.io.Info;
import static bsd.leveldb.db.Versions.*;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.FileName.*;
import static bsd.leveldb.Status.Code.*;

/**
 * The representation of a DBImpl consists of a set of Versions.
 *
 * The newest version is called "current".
 * Older versions may be kept around to provide a consistent view to live iterators.
 *
 * Each Version keeps track of a set of Table files per level.
 * The entire set of versions is maintained in a VersionSet.
 *
 * Version,VersionSet are thread-compatible, but require external synchronization on all accesses.
 */
class VersionSet implements Closeable {

    Env env;
    Path dbname;
    //   const Options* const options_;
    boolean paranoidChecks, reuseLogs;
    int maxFileSize;

    TableCache tableCache;
    InternalKeyComparator icmp;

    long nextFileNumber;
    long manifestFileNumber;
    long lastSequence;
    long logNumber;
    long prevLogNumber;  // 0 or backing store for memtable being compacted

    // Opened lazily
    LogWriter descriptorLog;  // log::Writer* descriptor_log_;
    // WritableFile* descriptor_file_;

    Map<Version,Void> dummyVersions;  // Head of circular doubly-linked list of versions.
    Version current;                  // == dummy_versions_.prev_

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    InternalKey[] compactPointer = new InternalKey[kNumLevels];

    VersionSet(Path dbname, Env env) {
        this.dbname = dbname;
        this.env = env;
    }

    VersionSet comparator(InternalKeyComparator cmp) {
        icmp = cmp; return this;
    }
    VersionSet paranoidChecks(boolean check) {
        paranoidChecks = check; return this;
    }
    VersionSet files(boolean reuse, int max) {
        reuseLogs = reuse; maxFileSize = max; return this;
    }
    VersionSet cache(TableCache cache) {
        tableCache = cache; return this;
    }

    // VersionSet::VersionSet(...)
    VersionSet open() {
        nextFileNumber = 2;
        manifestFileNumber = 0; // Filled by Recover()
        lastSequence = 0;
        logNumber = 0;
        prevLogNumber = 0;
        // descriptor_file_(NULL),
        descriptorLog = null;
        dummyVersions = new WeakHashMap<>(); // dummy_versions_(this)
        current = null;
        appendVersion(new Version(this));
        return this;
    }

    @Override
    public void close() { // VersionSet::~VersionSet()
        current.unref();
        // assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
        descriptorLog.close(); // delete descriptor_log_;
        // delete descriptor_file_;
    }

    // Return the current version.
    Version current() {
        return current;
    }

    // Return the current manifest file number.
    long manifestFileNumber() {
        return manifestFileNumber;
    }

    // Allocate and return a new file number.
    long newFileNumber() {
        return nextFileNumber++;
    }

    // Arrange to reuse "file_number"
    // unless a newer file number has already been allocated.
    // REQUIRES: "file_number" was returned by a call to NewFileNumber().
    void reuseFileNumber(long fileNumber) {
        if (nextFileNumber == fileNumber + 1) {
            nextFileNumber = fileNumber;
        }
    }

    // Return the number of Table files at the specified level.
    int numLevelFiles(int level) {
        assert (level >= 0);
        assert (level < kNumLevels);
        return current.files[level].size();
    }

    // Return the combined file size of all files at the specified level.
    long numLevelBytes(int level) {
        assert (level >= 0);
        assert (level < kNumLevels);
        return totalFileSize(current.files[level]);
    }

    // Return the last sequence number.
    long lastSequence() {
        return lastSequence;
    }

    // Set the last sequence number to s.
    void setLastSequence(long s) {
        assert (s >= lastSequence);
        lastSequence = s;
    }

    // Mark the specified file number as used.
    void markFileNumberUsed(long number) {
        if (nextFileNumber <= number) {
            nextFileNumber = number + 1;
        }
    }

    // Return the current log file number.
    long logNumber() {
        return logNumber;
    }

    // Return the log file number for the log file that is currently being compacted,
    // or zero if there is no such log file.
    long prevLogNumber() {
        return prevLogNumber;
    }

    // options->max_file_size;
    int targetFileSize() {
        return maxFileSize;
    }

    // Maximum bytes of overlaps in grandparent (i.e., level+2)
    // before we stop building a single file in a level->level+1 compaction.
    long maxGrandParentOverlapBytes() {
        return 10 * targetFileSize();
    }

    // Maximum number of bytes in all compacted files.
    // We avoid expanding the lower level file set of a compaction
    // if it would make the total compaction cover more than this many bytes.
    long expandedCompactionByteSizeLimit() {
        return 25 * targetFileSize();
    }

    double maxBytesForLevel(int level) {
        // Note: the result for level zero is not really used since we set
        // the level-0 compaction threshold based on number of files.

        // Result for both level-0 and level-1
        double result = 10. * 1048576.0;
        while (level > 1) {
            result *= 10;
            level--;
        }
        return result;
    }

    long maxFileSizeForLevel(int level) {
        // We could vary per level to reduce number of files?
        return targetFileSize();
    }

    long totalFileSize(List<FileMetaData> files) {
        long sum = 0;
        for (int i = 0; i < files.size(); i++) {
            sum += files.get(i).fileSize;
        }
        return sum;
    }

//  struct LogReporter : public log::Reader::Reporter {
//    Status* status;
//    virtual void Corruption(size_t bytes, const Status& s) {
//      if (this->status->ok()) *this->status = s;
//    }
//  };

//  LogReporter reporter;
//  reporter.status = &s;

    /**
     * Recover the last saved descriptor from persistent storage.
     */
    void recover(Struct.Bool saveManifest) { // Status Recover(bool *save_manifest);

        // Read "CURRENT" file, which contains a pointer to the current manifest file
        Path dscname;
        InputStream file;
        try {
            Path fname = currentFileName(dbname);
            byte[] dscbase = env.readFileToString(fname);
            if (dscbase.length == 0 || dscbase[dscbase.length-1] != '\n') {
                throw new Status("CURRENT file does not end with newline").state(Corruption);
            }
            dscname = dbname.resolve(new String(dscbase).trim());
            file = env.newSequentialFile(dscname);
        }
        catch (IOException e) { throw new Status(e).state(IOError); }

        boolean haveLogNumber = false;
        boolean havePrevLogNumber = false;
        boolean haveNextFile = false;
        boolean haveLastSequence = false;
        long nextFile = 0;
        long lastSeq = 0;
        long logNum = 0;
        long prevLogNum = 0;

        Builder builder = new Builder(this,current);
        try {
            LogReader reader = new LogReader(file);
            // log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
            for (Slice record : reader) {
                VersionEdit edit = new VersionEdit();
                edit.decodeFrom(record);

                String icmpName = icmp.userComparator.name();
                if (edit.hasComparator && ! edit.comparator.equals(icmpName)) {
                    throw new Status(edit.comparator + " does not match existing comparator " + icmpName)
                                    .state(InvalidArgument);
                }

                builder.apply(edit);

                if (edit.hasLogNumber) {
                    logNum = edit.logNumber;
                    haveLogNumber = true;
                }
                if (edit.hasPrevLogNumber) {
                    prevLogNum = edit.prevLogNumber;
                    havePrevLogNumber = true;
                }
                if (edit.hasNextFileNumber) {
                    nextFile = edit.nextFileNumber;
                    haveNextFile = true;
                }
                if (edit.hasLastSequence) {
                    lastSeq = edit.lastSequence;
                    haveLastSequence = true;
                }
            }

            file.close(); // delete file;
        }
        catch (IOException e) { throw new Status(e).state(IOError); }

        if (!haveNextFile) {
            throw new Status("no meta-nextfile entry in descriptor").state(Corruption);
        } else if (!haveLogNumber) {
            throw new Status("no meta-lognumber entry in descriptor").state(Corruption);
        } else if (!haveLastSequence) {
            throw  new Status("no last-sequence-number entry in descriptor").state(Corruption);
        }

        if (!havePrevLogNumber) {
            prevLogNumber = 0;
        }

        markFileNumberUsed(prevLogNumber);
        markFileNumberUsed(logNumber);

        Version v = new Version(this);
        builder.saveTo(v);
        builder.close();
        // Install recovered version
        finalize(v);
        appendVersion(v);
        manifestFileNumber = nextFile;
        nextFileNumber = nextFile + 1;
        lastSequence = lastSeq;
        logNumber = logNum;
        prevLogNumber = prevLogNum;

        // See if we can reuse the existing MANIFEST file.
        if (reuseManifest(dscname)) {
            // No need to save new manifest
        } else {
            saveManifest.v = true; // *save_manifest = true;
        }
    }

    void finalize(Version v) {
        // Precomputed best level for next compaction
        int bestLevel = -1;
        double bestScore = -1;

        for (int level = 0; level < kNumLevels-1; level++) {
            double score;
            if (level == 0) {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:

                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.

                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer setting,
                // or very high compression ratios, or lots of overwrites/deletions).

                score = (double) v.files[level].size() /
                        (double) kL0_CompactionTrigger;
            } else {
                // Compute the ratio of current size to size limit.
                long levelBytes = totalFileSize(v.files[level]);
                score =
                    (double) levelBytes / maxBytesForLevel(level);
            }

            if (score > bestScore) {
                bestLevel = level;
                bestScore = score;
            }
        }

        v.compactionLevel = bestLevel;
        v.compactionScore = bestScore;
    }

    final void appendVersion(Version v) {
        // Make "v" current
        assert (v.refs == 0);
        assert (v != current);
        if (current != null) {
            current.unref();
        }
        current = v;
        v.ref();

        // Append to linked list
        dummyVersions.put(v,null);
        // v->prev_ = dummy_versions_.prev_;
        // v->next_ = &dummy_versions_;
        // v->prev_->next_ = v;
        // v->next_->prev_ = v;
    }

    void removeVersion(Version v) {
        dummyVersions.remove(v);
    }

    boolean reuseManifest(Path dscname) {
        if (!reuseLogs) {
            return false;
        }
        ParsedFileName manifest = parseFileName(dscname);
        if (manifest == null || manifest.type != FileType.kDescriptorFile) {
            return false;
        }
        try {
            long manifestSize = env.getFileSize(dscname);
            // Make new compacted MANIFEST if old one is too big
            if (manifestSize >= targetFileSize()) {
                return false;
            }
        } catch (IOException ignore) {
            return false;
        }

        // assert(descriptor_file_ == NULL);
        assert (descriptorLog == null);
        try {
            OutputStream descriptorFile = env.newAppendableFile(dscname);
            descriptorLog = new LogWriter(descriptorFile);
            Info.log( "Reusing MANIFEST %s", dscname.toString() );
        }
        catch (IOException r) {
            Info.log( "Reuse MANIFEST: %s", r.toString() );
            descriptorLog = null;
            // assert(descriptor_file_ == NULL);
            return false;
        }
        manifestFileNumber = manifest.number;
        return true;
    }

    /**
     * Add all files listed in any live version to *live.
     * May also mutate some internal state.
     */
    void addLiveFiles(Set<Long> live) {
        for (Version v : dummyVersions.keySet()) {
            for (int level = 0; level < kNumLevels; level++) {
                List<FileMetaData> files = ((Version)v).files[level];
                for (int i = 0; i < files.size(); i++) {
                    live.add(files.get(i).number);
                }
            }
        }
    }

    /**
     * Apply *edit to the current version to form a new descriptor that
     * is both saved to persistent state and installed as the new current version.
     * Will release *mu while actually writing to the file.
     * // REQUIRES: *mu is held on entry.
     * // REQUIRES: no other thread concurrently calls LogAndApply()
     */
    void logAndApply(VersionEdit edit, ReentrantLock mu) { // Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
        if (edit.hasLogNumber) {
            assert (edit.logNumber >= logNumber);
            assert (edit.logNumber < nextFileNumber);
        } else {
            edit.setLogNumber(logNumber);
        }

        if (!edit.hasPrevLogNumber) {
            edit.setPrevLogNumber(prevLogNumber);
        }

        edit.setNextFile(nextFileNumber);
        edit.setLastSequence(lastSequence);

        Version v = new Version(this);
        {
            Builder builder = new Builder(this,current);
            builder.apply(edit);
            builder.saveTo(v);
            builder.close();
        }
        finalize(v);

        // Initialize new descriptor log file if necessary by creating
        // a temporary file that contains a snapshot of the current version.
        boolean newManifestFile = false;
        if (descriptorLog == null) {
            // No reason to unlock *mu here since we only hit this path in the
            // first call to LogAndApply (when opening the database).
            // assert(descriptor_file_ == NULL);
            Path manifestFile = descriptorFileName(dbname, manifestFileNumber);
            edit.setNextFile(nextFileNumber);//     if (s.ok()) {
            try {
                OutputStream descriptorFile = env.newWritableFile(manifestFile);
                descriptorLog = new LogWriter(descriptorFile);
                writeSnapshot(descriptorLog);
                newManifestFile = true;
            }
            catch (IOException e) {
                throw new Status(e).state(IOError);
            }
        }

        // Unlock during expensive MANIFEST log write
        mu.unlock();
        try {
            // Write new record to MANIFEST log
            Slice record = new Slice(edit.encodeTo());
            descriptorLog.addRecord(record);
            env.syncFile(descriptorLog.out);

            // If we just created a new descriptor file, install it by writing a
            // new CURRENT file that points to it.
            if (newManifestFile) {
                setCurrentFile(env,dbname,manifestFileNumber);
            }
        }
        catch (IOException e) {
            Info.log( "MANIFEST write: %s", e.toString() );
            v.close(); // delete v;
            // if (!new_manifest_file.empty()) {
            //   delete descriptor_log_;
            //   delete descriptor_file_;
            //   descriptor_log_ = NULL;
            //   descriptor_file_ = NULL;
            //   env_->DeleteFile(new_manifest_file);
            // }
            throw new Status(e).state(IOError);
        }
        finally {
            mu.lock();
        }

        // Install the new version
        appendVersion(v);
        logNumber = edit.logNumber;
        prevLogNumber = edit.prevLogNumber;
    }

    /**
     * Save current contents to *log.
     */
    void writeSnapshot(LogWriter log) {

        // Save metadata
        VersionEdit edit = new VersionEdit();
        edit.setComparatorName(icmp.userComparator.name());

        // Save compaction pointers
        for (int level = 0; level < kNumLevels; level++) {
            InternalKey key = compactPointer[level];
            if (key != null) {
                edit.setCompactPointer(level, key);
            }
        }

        // Save files
        for (int level = 0; level < kNumLevels; level++) {
            List<FileMetaData> files = current.files[level];
            for (int i = 0; i < files.size(); i++) {
                FileMetaData f = files.get(i);
                edit.addFile(level, f.number, f.fileSize, f.smallest, f.largest );
            }
        }

        Slice record = new Slice(edit.encodeTo());
        log.addRecord(record);
    }
    // TODO: Break up into multiple records to reduce memory usage on recovery?

    /**
     * Returns true iff some level needs a compaction.
     */
    boolean needsCompaction() {
        Version v = current;
        return (v.compactionScore >= 1) || (v.fileToCompact != null);
    }

    /**
     * Return a human-readable short (single-line) summary of the number of files per level.
     * Uses *scratch as backing store.
     */
    String levelSummary() {
        StringBuilder b = new StringBuilder();
        b.append("files[ ");
        for (int i = 0; i < current.files.length; i++) {
            b.append(current.files[i].size()).append(' ');
        }
        b.append(']');
        return b.toString();
    }



    /**
     * Return a compaction object for compacting the range [begin,end] in the specified level.
     * Returns NULL if there is nothing in that level that overlaps the specified range.
     * Caller should delete the result.
     */
    Compaction compactRange(int level, InternalKey begin, InternalKey end) {
        List<FileMetaData> inputs =
            current.getOverlappingInputs(level, begin, end);
            if (inputs.isEmpty()) {
                return null;
        }

        // Avoid compacting too much in one shot in case the range is large.
        // But we cannot do this for level-0 since level-0 files can overlap
        // and we must not pick one file and drop another older file
        // if the two files overlap.
        if (level > 0) {
            long limit = maxFileSizeForLevel(level);
            long total = 0;
            for (int i = 0; i < inputs.size(); i++) {
                long s = inputs.get(i).fileSize;
                total += s;
                if (total >= limit) {
                    inputs.add(new FileMetaData()); // inputs.resize(i + 1);
                    break;
                }
            }
        }

        Compaction c = new Compaction(level, maxFileSizeForLevel(level));
        c.inputVersion = current;
        c.inputVersion.ref();
        c.inputs[0] = inputs;
        c.setupOtherInputs(this);
        return c;
    }

    /**
     * Return the maximum overlapping data (in bytes) at next level for any file at a level >= 1.
     */
    long maxNextLevelOverlappingBytes() { // int64_t VersionSet::MaxNextLevelOverlappingBytes()
        long result = 0;
        List<FileMetaData> overlaps;
        for (int level = 1; level < kNumLevels - 1; level++) {
            for (int i = 0; i < current.files[level].size(); i++) {
                FileMetaData f = current.files[level].get(i);
                overlaps = current.getOverlappingInputs(level+1, f.smallest, f.largest);
                long sum = totalFileSize(overlaps);
                if (sum > result) {
                    result = sum;
                }
            }
        }
        return result;
    }

    long approximateOffsetOf(Version v, InternalKey k1) { // TODO: for DbUtil.getApproximateSize(Slice start, Slice limit)
        return -1;
    }

}



//   // Return the approximate offset in the database of the data for
//   // "key" as of version "v".
//   uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);
// uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
//   uint64_t result = 0;
//   for (int level = 0; level < config::kNumLevels; level++) {
//     const std::vector<FileMetaData*>& files = v->files_[level];
//     for (size_t i = 0; i < files.size(); i++) {
//       if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
//         // Entire file is before "ikey", so just add the file size
//         result += files[i]->file_size;
//       } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
//         // Entire file is after "ikey", so ignore
//         if (level > 0) {
//           // Files other than level 0 are sorted by meta->smallest, so
//           // no further files in this level will contain data for
//           // "ikey".
//           break;
//         }
//       } else {
//         // "ikey" falls in the range for this table.  Add the
//         // approximate offset of "ikey" within the table.
//         Table* tableptr;
//         Iterator* iter = table_cache_->NewIterator(
//             ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
//         if (tableptr != NULL) {
//           result += tableptr->ApproximateOffsetOf(ikey.Encode());
//         }
//         delete iter;
//       }
//     }
//   }
//   return result;
// }
