package bsd.leveldb.db;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import bsd.leveldb.Slice;
import bsd.leveldb.Cursor;
import bsd.leveldb.io.Info;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.VersionEdit.*;

class Versions {

    /**
     * Pick level and inputs for a new compaction.
     * Returns NULL if there is no compaction to be done.
     * Otherwise returns a pointer to a heap-allocated object that describes the compaction.
     * Caller should delete the result.
     */
    static Compaction pickCompaction(VersionSet vset) { // Compaction* VersionSet::PickCompaction() {
        Compaction c;
        int level;

        // We prefer compactions triggered by too much data in a level
        // over the compactions triggered by seeks.
        Version current = vset.current();
        boolean sizeCompaction = (current.compactionScore >= 1);
        boolean seekCompaction = (current.fileToCompact != null);
        if (sizeCompaction) {
            level = current.compactionLevel;
            assert (level >= 0);
            assert (level+1 < kNumLevels);
            c = new Compaction(level,vset.maxFileSizeForLevel(level));

            // Pick the first file that comes after compact_pointer_[level]
            for (int i = 0; i < current.files[level].size(); i++) {
                FileMetaData f = current.files[level].get(i);
                if (vset.compactPointer[level] == null ||
                    vset.icmp.compare(f.largest, vset.compactPointer[level]) > 0)
                {
                    c.inputs[0].add(f);
                    break;
                }
            }
            if (c.inputs[0].isEmpty()) {
                // Wrap-around to the beginning of the key space
                c.inputs[0].add(current.files[level].get(0));
            }
        } else if (seekCompaction) {
            level = current.fileToCompactLevel;
            c = new Compaction(level,vset.maxFileSizeForLevel(level));
            c.inputs[0].add(current.fileToCompact);
        } else {
            return null;
        }

        c.inputVersion = current;
        c.inputVersion.ref();

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if (level == 0) {
            InternalKey[] r = // [smallest,largest]
                getRange(c.inputs[0], vset.icmp);
            // Note that the next call will discard the file we placed in
            // c->inputs_[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            c.inputs[0] = current.getOverlappingInputs(0, r[small], r[large]);
            assert (!c.inputs[0].isEmpty());
        }

        c.setupOtherInputs(vset); // SetupOtherInputs(c);

        return c;
    }

    // Stores the minimal range that covers all entries in inputs in
    // *smallest, *largest.
    // REQUIRES: inputs is not empty
    static InternalKey[] getRange(List<FileMetaData> inputs, Comparator<InternalKey> icmp) {
        assert (!inputs.isEmpty());
        InternalKey smallest = null;
        InternalKey largest = null;
        for (int i = 0; i < inputs.size(); i++) {
            FileMetaData f = inputs.get(i);
            if (i == 0) {
                smallest = f.smallest;
                largest = f.largest;
            } else {
                if (icmp.compare(f.smallest, smallest) < 0) {
                    smallest = f.smallest;
                }
                if (icmp.compare(f.largest, largest) > 0) {
                    largest = f.largest;
                }
            }
        }
        return new InternalKey[]{smallest,largest};
    }

    static final int small = 0;
    static final int large = 1;

    // Stores the minimal range that covers all entries in inputs1 and inputs2
    // in *smallest, *largest.
    // REQUIRES: inputs is not empty
    static InternalKey[] getRange2(List<FileMetaData> inputs1, List<FileMetaData> inputs2, Comparator<InternalKey> icmp) {
        List<FileMetaData> inputs = new ArrayList<>( inputs1.size() + inputs2.size() );
        inputs.addAll(inputs1);
        inputs.addAll(inputs2);
        return getRange(inputs,icmp);
    }


  /**
   * A Compaction encapsulates information about a compaction.
   */
  static class Compaction implements Closeable {

    int level;
    long maxOutputFileSize;
    Version inputVersion;
    VersionEdit edit = new VersionEdit();

    // Each compaction reads inputs from "level_" and "level_+1"
    List<FileMetaData>[] inputs; // The two sets of inputs
    // std::vector<FileMetaData*> inputs_[2];

    // State used to check for number of of overlapping grandparent files
    // (parent == level_ + 1, grandparent == level_ + 2)

    List<FileMetaData> grandparents;
    int grandparentIndex;  // Index in grandparent_starts_
    boolean seenKey;       // Some output key has been seen
    long overlappedBytes;  // Bytes of overlap between current output
                           // and grandparent files

    // State for implementing IsBaseLevelForKey

    // level_ptrs_ holds indices into input_version_->levels_:
    // our state is that we are positioned at one of the file ranges
    // for each higher level than the ones involved in this compaction
    // (i.e. for all L >= level_ + 2).
    int[] levelPtrs = new int[kNumLevels];

    Compaction(int level, long maxOutputFileSize) { // const Options* options, int level)
        this.level = level; //     : level_(level),
        this.maxOutputFileSize = maxOutputFileSize; //  max_output_file_size_(MaxFileSizeForLevel(options, level)),
        inputVersion = null; //       input_version_(NULL),
        grandparentIndex = 0; //       grandparent_index_(0),
        seenKey = false; //       seen_key_(false),
        overlappedBytes = 0; //       overlapped_bytes_(0) {
        for (int i = 0; i < kNumLevels; i++) {
            levelPtrs[i] = 0;
        }
        inputs = new List[]{ new ArrayList<>(), new ArrayList<>()};
        grandparents = new ArrayList<>();
    }

    @Override
    public void close() { // Compaction::~Compaction()
        if (inputVersion != null) {
            inputVersion.unref();
        }
    }

    // Return the level that is being compacted.
    // Inputs from "level" and "level+1" will be merged to produce a set of "level+1" files.
    int level() { return level; }

    // Return the object that holds the edits to the descriptor done by this compaction.
    VersionEdit edit() { return edit; }

    // "which" must be either 0 or 1
    int numInputFiles(int which) { return inputs[which].size(); }

    // Return the ith input file at "level()+which" ("which" must be 0 or 1).
    FileMetaData input(int which, int i) { return inputs[which].get(i); }

    // Maximum size of files to build during this compaction.
    long maxOutputFileSize() { return maxOutputFileSize; }

    // Is this a trivial compaction that can be implemented by just
    // moving a single input file to the next level (no merging or splitting)
    boolean isTrivialMove() {
        VersionSet vset = inputVersion.vset;
        // Avoid a move if there is lots of overlapping grandparent data.
        // Otherwise, the move could create a parent file
        //    that will require a very expensive merge later on.
        return (numInputFiles(0) == 1 && numInputFiles(1) == 0 &&
                vset.totalFileSize(grandparents) <=
                    vset.maxGrandParentOverlapBytes());
    }

    // Add all inputs to this compaction as delete operations to *edit.
    void addInputDeletions(VersionEdit edit) {
        for (int which = 0; which < 2; which++) {
            for (int i = 0; i < inputs[which].size(); i++) {
                edit.deleteFile(level + which, inputs[which].get(i).number);
            }
        }
    }

    // Returns true if the information we have available guarantees that the compaction
    // is producing data in "level+1" for which no data exists in levels greater than "level+1".
    boolean isBaseLevelForKey(Slice userKey) {
        // Maybe use binary search to find right entry instead of linear search?
        Comparator userCmp = inputVersion.vset.icmp.userComparator;
        for (int lvl = level + 2; lvl < kNumLevels; lvl++) {
            List<FileMetaData> files = inputVersion.files[lvl];
            while (levelPtrs[lvl] < files.size()) {
                FileMetaData f = files.get(levelPtrs[lvl]);
                if (userCmp.compare(userKey, f.largest.userKey) <= 0) {
                    // We've advanced far enough
                    if (userCmp.compare(userKey, f.smallest.userKey) >= 0) {
                        // Key falls in this file's range, so definitely not base level
                        return false;
                    }
                    break;
                }
                levelPtrs[lvl]++;
            }
        }
        return true;
    }

    // Returns true iff we should stop building the current output before processing "internal_key".
    boolean shouldStopBefore(InternalKey internalKey) {
        VersionSet vset = inputVersion.vset;
        // Scan to find earliest grandparent file that contains key.
        InternalKeyComparator icmp = vset.icmp;
        while (grandparentIndex < grandparents.size() &&
               icmp.compare(internalKey,
                    grandparents.get(grandparentIndex).largest) > 0) {
            if (seenKey) {
                overlappedBytes += grandparents.get(grandparentIndex).fileSize;
            }
            grandparentIndex++;
        }
        seenKey = true;

        if (overlappedBytes > vset.maxGrandParentOverlapBytes()) {
            // Too much overlap for current output; start new output
            overlappedBytes = 0;
            return true;
        } else {
            return false;
        }
    }

    // Release the input version for the compaction, once the compaction is successful.
    void releaseInputs() {
        if (inputVersion != null) {
            inputVersion.unref();
            inputVersion = null;
        }
    }

    // void VersionSet::SetupOtherInputs(Compaction* c)
    void setupOtherInputs(VersionSet vset) {
        Version current = inputVersion;
        int level = level();

        InternalKey[] r = getRange(inputs[0],vset.icmp);
        InternalKey smallest=r[0], largest=r[1];

        inputs[1] = current.getOverlappingInputs(level+1, smallest, largest);

        // Get entire range covered by compaction
        InternalKey[] a = getRange2(inputs[0], inputs[1], vset.icmp);
        InternalKey all_start = a[0], all_limit = a[1];

        // See if we can grow the number of inputs in "level" without
        // changing the number of "level+1" files we pick up.
        if (!inputs[1].isEmpty()) {
            List<FileMetaData> expanded0 =
                current.getOverlappingInputs(level, all_start, all_limit);
            long inputs0_size = vset.totalFileSize(inputs[0]);
            long inputs1_size = vset.totalFileSize(inputs[1]);
            long expanded0_size = vset.totalFileSize(expanded0);
            if (expanded0.size() > inputs[0].size() &&
                inputs1_size + expanded0_size < vset.expandedCompactionByteSizeLimit())
            {
                InternalKey[] n = getRange(expanded0, vset.icmp);
                InternalKey new_start=n[0], new_limit=n[1];
                List<FileMetaData> expanded1 =
                    current.getOverlappingInputs(level+1, new_start, new_limit);
                if (expanded1.size() == inputs[1].size()) {
                    Info.log(
                        "Expanding @%d %d+%d (%d+%d bytes) to %d+%d (%d+%d bytes)",
                            level,
                            inputs[0].size(),
                            inputs[1].size(),
                            inputs0_size, inputs1_size,
                            expanded0.size(),
                            expanded1.size(),
                            expanded0_size, inputs1_size);
                    smallest = new_start;
                    largest = new_limit;
                    inputs[0] = expanded0;
                    inputs[1] = expanded1;
                    a = getRange2(inputs[0],inputs[1],vset.icmp);
                    all_start = a[0]; all_limit = a[1];
                    // , c->inputs_[1], &all_start, &all_limit);
                }
            }
        }

        // Compute the set of grandparent files that overlap this compaction
        // (parent == level+1; grandparent == level+2)
        if (level + 2 < kNumLevels) {
            grandparents =
                current.getOverlappingInputs(level + 2, all_start, all_limit);
        }

        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        vset.compactPointer[level] = largest;
        edit.setCompactPointer(level, largest);
    }

    /**
     * Create an iterator that reads over the compaction inputs for "*c".
     * The caller should delete the iterator when no longer needed.
     */
    Cursor<InternalKey,Slice> makeInputIterator(VersionSet vset) { // Iterator* MakeInputIterator(Compaction* c);
        // ReadOptions options;
        // options.verify_checksums = options_->paranoid_checks;
        boolean fillCache = false;

        // Level-0 files have to be merged together.
        // For other levels, we will make a concatenating iterator per level.
        // TODO(opt): use concatenating iterator for level-0 if there is no overlap

        int space = (level() == 0 ? inputs[0].size() + 1 : 2);
        Cursor<InternalKey,Slice>[] list = new Cursor[space];
        int num = 0;
        for (int which = 0; which < 2; which++) {
            if (!inputs[which].isEmpty()) {
                if (level() + which == 0) {
                    List<FileMetaData> files = inputs[which];
                    for (int i = 0; i < files.size(); i++) {
                        FileMetaData f = files.get(i);
                        list[num++] = vset.tableCache.newIterator(f.number,f.fileSize,fillCache);
                        // list[num++] = table_cache_->NewIterator(
                        //     options, files[i]->number, files[i]->file_size);
                    }
                } else {
                    // Create concatenating iterator for the files from this level
                    list[num++] = new TwoLevelIterator<>(
                        inputs[which].iterator(),
                        (FileMetaData f) -> vset.tableCache.newIterator(f.number,f.fileSize,fillCache)
                    );
                    // list[num++] = NewTwoLevelIterator(
                    //     new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
                    //     &GetFileIterator, table_cache_, options);
                }
            }
        } // for()
        assert (num <= space);
        Cursor<InternalKey,Slice> result = MergingIterator.of(vset.icmp, list, num);
        // Iterator* result = NewMergingIterator(&icmp_, list, num);
        // delete[] list;
        return result;
    }

    //  Iterator* GetFileIterator(void* arg,
    //                            const ReadOptions& options,
    //                            const Slice& file_value) {
    //    TableCache* cache = reinterpret_cast<TableCache*>(arg);
    //    if (file_value.size() != 16) {
    //      return NewErrorIterator(
    //      Status::Corruption("FileReader invoked with unexpected value"));
    //    } else {
    //      return cache->NewIterator(options,
    //                                DecodeFixed64(file_value.data()),
    //                                DecodeFixed64(file_value.data() + 8));
    //    }
    //  }

  } // Compaction


  /**
   * A helper class so we can efficiently apply a whole sequence
   * of edits to a particular state without creating intermediate
   * Versions that contain full copies of the intermediate state.
   */
  static class Builder implements Closeable {

    VersionSet vset;
    Version base;

    LevelState[] levels = new LevelState[kNumLevels];

    class LevelState {
        final Set<Long> deletedFiles = new HashSet<>();
        final SortedSet<FileMetaData> addedFiles = new TreeSet<>(bySmallestKey);
    }

    // Helper to sort by v->files_[file_number].smallest
    final Comparator<FileMetaData> bySmallestKey = new Comparator<FileMetaData>(){
        @Override
        public int compare(FileMetaData f1, FileMetaData f2) {
            int r = vset.icmp.compare(f1.smallest,f2.smallest);
            if (r != 0) {
                return r;
            } else {
                // Break ties by file number
                return (int) (f1.number - f2.number);
            }
        }
    };
    //   struct BySmallestKey {
    //     const InternalKeyComparator* internal_comparator;
    //
    //     bool operator()(FileMetaData* f1, FileMetaData* f2) const {
    //       int r = internal_comparator->Compare(f1->smallest, f2->smallest);
    //       if (r != 0) {
    //         return (r < 0);
    //       } else {
    //         // Break ties by file number
    //         return (f1->number < f2->number);
    //       }
    //     }
    //   };

    // Initialize a builder with the files from *base and other info from *vset
    Builder(VersionSet vset, Version base) {
        this.vset = vset;
        this.base = base;
        base.ref();
        for (int level = 0; level < kNumLevels; level++) {
            levels[level] = new LevelState();
        }
    } // Builder()

    @Override
    public void close() { // ~Builder() // TODO: 
        // for (int level = 0; level < config::kNumLevels; level++) {
        //   const FileSet* added = levels_[level].added_files;
        //   std::vector<FileMetaData*> to_unref;
        //   to_unref.reserve(added->size());
        //   for (FileSet::const_iterator it = added->begin();
        //     it != added->end(); ++it) {
        //     to_unref.push_back(*it);
        //   }
        //   delete added;
        //   for (uint32_t i = 0; i < to_unref.size(); i++) {
        //     FileMetaData* f = to_unref[i];
        //     f->refs--;
        //     if (f->refs <= 0) {
        //       delete f;
        //     }
        //   }
        // }
        base.unref();
    }
    // TODO: VersionSet.Builder.close()

    // Apply all of the edits in *edit to the current state.
    void apply(VersionEdit edit) {

        // Update compaction pointers
        for (int i = 0; i < edit.compactPointers.size(); i++) {
            Object[] compactPointer = edit.compactPointers.get(i);
            int level = (int) compactPointer[first];
            vset.compactPointer[level] =
                (InternalKey) compactPointer[second];
        }

        // Delete files
        for (String iter : edit.deletedFiles) {
            long[] deletedFile = deletedFile(iter);
            int level = (int) deletedFile[first];
            long number = (long) deletedFile[second];
            levels[level].deletedFiles.add(number);
        }

        // Add new files
        for (int i = 0; i < edit.newFiles.size(); i++) {
            Object[] newFile = edit.newFiles.get(i);
            int level = (int) newFile[first];
            FileMetaData f = (FileMetaData) newFile[second];
            f.refs = 1;

            // We arrange to automatically compact this file after a certain number of seeks.

            // Let's assume:
            //   (1) One seek costs 10ms
            //   (2) Writing or reading 1MB costs 10ms (100MB/s)
            //   (3) A compaction of 1MB does 25MB of IO:
            //         1MB read from this level
            //         10-12MB read from next level (boundaries may be misaligned)
            //         10-12MB written to next level

            // This implies that 25 seeks cost the same as the compaction of 1MB of data.
            // I.e., one seek costs approximately the same as the compaction of 40KB of data.
            // We are a little conservative and allow approximately one seek for every 16KB
            // of data before triggering a compaction.

            f.allowedSeeks = (int)(f.fileSize / 16384);
            if (f.allowedSeeks < 100) f.allowedSeeks = 100;

            levels[level].deletedFiles.remove(f.number);
            levels[level].addedFiles.add(f);
        }
    }

    /**
     * Save the current state in *v.
     */
    void saveTo(Version v) {
        for (int level = 0; level < kNumLevels; level++) {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.  Store the result in *v.
            List<FileMetaData> baseFiles = base.files[level];
            SortedSet<FileMetaData> addedFiles = levels[level].addedFiles;
            List<FileMetaData> sortedFiles = new ArrayList(baseFiles.size()+addedFiles.size());
            // Add all smaller files listed in base_
            sortedFiles.addAll(baseFiles);
            // Add remaining base files
            sortedFiles.addAll(addedFiles);

            sortedFiles.sort(addedFiles.comparator());
            for (FileMetaData f : sortedFiles) {
                maybeAddFile(v,level,f);
            }
        }
    }

// #ifndef NDEBUG
//       // Make sure there is no overlap in levels > 0
//       if (level > 0) {
//         for (uint32_t i = 1; i < v->files_[level].size(); i++) {
//           const InternalKey& prev_end = v->files_[level][i-1]->largest;
//           const InternalKey& this_begin = v->files_[level][i]->smallest;
//           if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
//             fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
//                     prev_end.DebugString().c_str(),
//                     this_begin.DebugString().c_str());
//             abort();
//           }
//         }
//       }
// #endif

    void maybeAddFile(Version v, int level, FileMetaData f) {
        if (levels[level].deletedFiles.contains(f.number)) {
            // File is deleted: do nothing
        } else {
            List<FileMetaData> files = v.files[level];
            if (level > 0 && !files.isEmpty()) {
                // Must not overlap
                assert (vset.icmp.compare(files.get(files.size()-1).largest, f.smallest) < 0);
            }
            f.refs++;
            files.add(f);
        }
    }

  } // Builder

} // Versions