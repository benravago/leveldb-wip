package bsd.leveldb.db;

import bsd.leveldb.Cursor;
import java.io.Closeable;

import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;

import bsd.leveldb.Slice;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.Struct.*;

class Version implements Closeable {

    VersionSet vset;            // VersionSet to which this Version belongs
    // Version* next_;          // Next version in linked list
    // Version* prev_;          // Previous version in linked list
    volatile int refs;          // Number of live refs to this version

    // List of files per level
    List<FileMetaData>[] files = repeat(ArrayList::new, kNumLevels); // std::vector<FileMetaData*> files_[config::kNumLevels];

    // Next file to compact based on seek stats.
    FileMetaData fileToCompact;
    int fileToCompactLevel;

    // Level that should be compacted next and its compaction score.
    // Score < 1 means compaction is not strictly needed.
    // These fields are initialized by Finalize().
    double compactionScore;
    int compactionLevel;

    Version(VersionSet vset) {
        this.vset = vset;
        // next_(this), prev_(this)
        refs = 0;
        fileToCompact = null;
        fileToCompactLevel = -1;
        compactionScore = -1;
        compactionLevel = -1;
    }

    @Override
    public void close() { // Version::~Version() {
        assert (refs == 0);

        // Remove from linked list
        vset.removeVersion(this);
        // prev_->next_ = next_;
        // next_->prev_ = prev_;

        // Drop references to files
        for (int level = 0; level < kNumLevels; level++) {
            for (int i = 0; i < files[level].size(); i++) {
                FileMetaData f = files[level].get(i);
                assert (f.refs > 0);
                f.refs--;
                if (f.refs <= 0) {
                    // delete f;
                }
            }
        }
    }

    /**
     * Reference count management
     * (so Versions do not disappear out from under live iterators).
     */
    void ref() {
        ++refs;
    }

    void unref() {
        // assert(this != &vset_->dummy_versions_);
        assert (refs >= 1);
        --refs;
        if (refs == 0) {
            close(); // delete this;
        }
    }

    // enum SaverState
    static final int
        kFound=0,
        kNotFound=1,
        kDeleted=2,
        kCorrupt=3,
        kInvalid=4;

    boolean recordReadSample(InternalKey key) { // TODO: for DbUtil.newIterator()
        return false;
    }

// // Callback from TableCache::Get()
// namespace {
// enum SaverState {
//   kNotFound,
//   kFound,
//   kDeleted,
//   kCorrupt,
// };
// struct Saver {
//   SaverState state;
//   const Comparator* ucmp;
//   Slice user_key;
//   std::string* value;
// };
// }
// static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
//   Saver* s = reinterpret_cast<Saver*>(arg);
//   ParsedInternalKey parsed_key;
//   if (!ParseInternalKey(ikey, &parsed_key)) {
//     s->state = kCorrupt;
//   } else {
//     if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
//       s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
//       if (s->state == kFound) {
//         s->value->assign(v.data(), v.size());
//       }
//     }
//   }
// }

    class GetStats {
        FileMetaData seekFile = null;
        int seekFileLevel = -1;
        // Get() result
        int state = kNotFound;
        Slice value = null;
    }

    /**
     * Adds "stats" into the current state.
     * Returns true if a new compaction may need to be triggered, false otherwise.
     * // REQUIRES: lock is held
     */
    boolean updateStats(GetStats stats) {
        FileMetaData f = stats.seekFile;
        if (f != null) {
            f.allowedSeeks--;
            if (f.allowedSeeks <= 0 && fileToCompact == null) {
                fileToCompact = f;
                fileToCompactLevel = stats.seekFileLevel;
                return true;
            }
        }
        return false;
    }

    /**
     * Lookup the value for key.
     * If found, store it in *val and return OK.
     * Else return a non-OK status.
     * Fills *stats.
     * // REQUIRES: lock is not held
     */
    GetStats get(Slice k, long sequenceNumber, boolean verifyChecksums, boolean fillCache )  { // Get(const ReadOptions&, const LookupKey& key, std::string* val, GetStats* stats);
        InternalKey ikey = lookupKey(k,sequenceNumber); // Slice ikey = k.internal_key();
        Slice userKey = k; // Slice user_key = k.user_key();
        KeyComparator<Slice> ucmp = vset.icmp.userComparator;
        GetStats stats = new GetStats();

        FileMetaData lastFileRead = null;
        int lastFileReadLevel = -1;

        // We can search level-by-level since entries never hop across levels.
        // Therefore we are guaranteed that if we find data in an smaller level,
        // later levels are irrelevant.
//   std::vector<FileMetaData*> tmp;
//   FileMetaData* tmp2;
        for (int level = 0; level < kNumLevels; level++) {
            int numFiles = files[level].size();
            if (numFiles == 0) continue;

            // Get the list of files to search in this level
            List<FileMetaData> listFiles = files[level]; //     FileMetaData* const* files = &files_[level][0];
            if (level == 0) {
                // Level-0 files may overlap each other.
                // Find all files that overlap user_key and
                // process them in order from newest to oldest.
                List<FileMetaData> tmp = new ArrayList<>(); // tmp.reserve(num_files);
                for (int i = 0; i < numFiles; i++) {
                    FileMetaData f = listFiles.get(i);
                    if (ucmp.compare(userKey, f.smallest.userKey) >= 0 &&
                        ucmp.compare(userKey, f.largest.userKey) <= 0)
                    {
                        tmp.add(f);
                    }
                }
                if (tmp.isEmpty()) continue;

                tmp.sort(newestFirst);
                listFiles = tmp;
                numFiles = tmp.size();
            } else {
                // Binary search to find earliest index whose largest key >= ikey.
                int index = findFile(vset.icmp, files[level], ikey);
                if (index >= numFiles) {
                    listFiles = null;
                    numFiles = 0;
                } else {
                    FileMetaData tmp2 = listFiles.get(index);
                    if (ucmp.compare(userKey, tmp2.smallest.userKey) < 0) {
                        // All of "tmp2" is past any data for user_key
                        listFiles = null;
                        numFiles = 0;
                    } else {
                        listFiles = List.of(tmp2);
                        numFiles = 1;
                    }
                }
            }
            if (listFiles == null) continue;
            for (int i = 0; i < numFiles; ++i) {
                if (lastFileRead != null && stats.seekFile == null) {
                    // We have had more than one seek for this read.  Charge the 1st file.
                    stats.seekFile = lastFileRead;
                    stats.seekFileLevel = lastFileReadLevel;
                }

                FileMetaData f = listFiles.get(i);
                lastFileRead = f;
                lastFileReadLevel = level;

                Entry<InternalKey,Slice> r =
                    vset.tableCache.get( f.number, f.fileSize,
                                         ikey, fillCache );
                // thrown 'Status' indicates kCorrupt state
                if (r == null) {
                    stats.state = kNotFound; // case kNotFound:
                    break; // Keep searching in other files
                } else {
                    if (valueType(r.getKey()) == kTypeValue) {
                        stats.state = kFound;
                        stats.value = r.getValue();
                    } else {
                        stats.state = kDeleted;
                    }
                    return stats;
                }
            } // for (numFiles)
        } // for (kNumLevels)

        return stats; // return Status::NotFound(Slice());  // Use an empty error message for speed
    }

    // static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
    //   return a->number > b->number;
    // }
    static Comparator<FileMetaData> newestFirst = (a,b) -> (int)( a.number - b.number );

    /**
     * Return the smallest index i such that files[i]->largest >= key.
     * Return files.size() if there is no such file.
     * // REQUIRES: "files" contains a sorted list of non-overlapping files.
     */
    static int findFile(InternalKeyComparator icmp, List<FileMetaData> files, InternalKey key) {
        int left = 0;
        int right = files.size();
        while (left < right) {
            int mid = (left + right) / 2;
            FileMetaData f = files.get(mid);
            if (icmp.compare(f.largest, key) < 0) {
                // Key at "mid.largest" is < "target".
                // Therefore all files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.largest" is >= "target".
                // Therefore all files after "mid" are uninteresting.
                right = mid;
            }
        }
        return right;
    }

    /**
     * Return the level at which we should place a new memtable compaction
     * result that covers the range [smallest_user_key,largest_user_key].
     */
    int pickLevelForMemTableOutput(Slice smallestUserKey, Slice largestUserKey) { //   int PickLevelForMemTableOutput(const Slice& smallest_user_key, const Slice& largest_user_key);
        int level = 0;
        if (!overlapInLevel(0, smallestUserKey, largestUserKey)) {
            // Push to next level if there is no overlap in next level,
            // and the #bytes overlapping in the level after that are limited.
            InternalKey start = internalKey(smallestUserKey,kMaxSequenceNumber,kValueTypeForSeek);
            InternalKey limit = internalKey(largestUserKey,0,0);
            while (level < kMaxMemCompactLevel) {
                if (overlapInLevel(level+1, smallestUserKey, largestUserKey)) {
                    break;
                }
                if (level+2 < kNumLevels) {
                    // Check that file does not overlap too many grandparent bytes.
                    List<FileMetaData> overlaps = getOverlappingInputs(level+2,start,limit);
                    long sum = vset.totalFileSize(overlaps);
                    if (sum > vset.maxGrandParentOverlapBytes()) {
                        break;
                    }
                }
                level++;
            }
        }
        return level;
    }

    /**
     * Returns true iff some file in the specified level overlaps
     * some part of [*smallest_user_key,*largest_user_key].
     * smallest_user_key==NULL represents a key smaller than all keys in the DB.
     * largest_user_key==NULL represents a key largest than all keys in the DB.
     */
    boolean overlapInLevel(int level, Slice smallestUserKey, Slice largestUserKey) {
        return someFileOverlapsRange( vset.icmp, (level > 0), files[level],
                                      smallestUserKey, largestUserKey );
    }

    /**
     * Returns true iff some file in "files" overlaps the user kOverlapsey range [*smallest,*largest].
     * smallest==NULL represents a key smaller than all keys in the DB.
     * largest==NULL represents a key largest than all keys in the DB.
     * // REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges in sorted order.
     */
    static boolean someFileOverlapsRange( InternalKeyComparator icmp,
        boolean disjointSortedFiles, List<FileMetaData> files,
        Slice smallestUserKey, Slice largestUserKey )
    {
        KeyComparator ucmp = icmp.userComparator;
        if (!disjointSortedFiles) {
            // Need to check against all files
            for (int i = 0; i < files.size(); i++) {
                FileMetaData f = files.get(i);
                if (afterFile(ucmp, smallestUserKey, f) ||
                    beforeFile(ucmp, largestUserKey, f))
                {
                    // No overlap
                } else {
                    return true;  // Overlap
                }
            }
            return false;
        }

        // Binary search over file list
        int index = 0;
        if (smallestUserKey != null) {
            // Find the earliest possible internal key for smallest_user_key
            InternalKey small = internalKey(smallestUserKey,kMaxSequenceNumber,kValueTypeForSeek);
            index = findFile(icmp, files, small);
        }

        if (index >= files.size()) {
            // beginning of range is after all files, so no overlap.
            return false;
        }

        return ! beforeFile(ucmp, largestUserKey, files.get(index));
    }

    static boolean afterFile(KeyComparator ucmp, Slice userKey, FileMetaData f) {
        // NULL user_key occurs before all keys and is therefore never after *f
        return (userKey != null &&
                ucmp.compare(userKey, f.largest.userKey) > 0);
    }

    static boolean beforeFile(KeyComparator ucmp, Slice userKey, FileMetaData f) {
        // NULL user_key occurs after all keys and is therefore never before *f
        return (userKey != null &&
                ucmp.compare(userKey, f.smallest.userKey) < 0);
    }

    // Store in "*inputs" all files in "level" that overlap [begin,end]
    // begin == NULL means before all keys
    // end == NULL means after all keys
    List<FileMetaData> getOverlappingInputs(int level, InternalKey begin, InternalKey end) {
        assert (level >= 0);
        assert (level < kNumLevels);
        List<FileMetaData> inputs = new ArrayList<>(); // inputs->clear();
        Slice userBegin = (begin != null) ? begin.userKey : null;
        Slice userEnd = (end != null) ? end.userKey : null;
        KeyComparator userCmp = vset.icmp.userComparator;
        for (int i = 0; i < files[level].size(); ) {
            FileMetaData f = files[level].get(i++);
            Slice fileStart = f.smallest.userKey;
            Slice fileLimit = f.largest.userKey;
            if (begin != null && userCmp.compare(fileLimit, userBegin) < 0) {
                // "f" is completely before specified range; skip it
            } else if (end != null && userCmp.compare(fileStart, userEnd) > 0) {
                // "f" is completely after specified range; skip it
            } else {
                inputs.add(f);
                if (level == 0) {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search.
                    if (begin != null && userCmp.compare(fileStart, userBegin) < 0) {
                        userBegin = fileStart;
                        inputs.clear();
                        i = 0;
                    } else if (end != null && userCmp.compare(fileLimit, userEnd) > 0) {
                        userEnd = fileLimit;
                        inputs.clear();
                        i = 0;
                    }
                }
            }
        }
        return inputs;
    }

    // Append to *iters a sequence of iterators that will
    // yield the contents of this Version when merged together.
    // REQUIRES: This version has been saved (see VersionSet::SaveTo)
    void addIterators(boolean fillCache, List<Cursor<InternalKey,Slice>> iters) {

        // Merge all level zero files together since they may overlap
        for (int i = 0; i < files[0].size(); i++) {
            iters.add(
                vset.tableCache.newIterator( // options.fillCache
                    files[0].get(i).number, files[0].get(i).fileSize, fillCache));
        }

        // For levels > 0, we can use a concatenating iterator that
        // sequentially walks through the non-overlapping files in the level,
        // opening them lazily.
        for (int level = 1; level < kNumLevels; level++) {
            if (!files[level].isEmpty()) {
                iters.add(newConcatenatingIterator(level,fillCache));
            }
        }
    }

    Cursor<InternalKey,Slice> newConcatenatingIterator(int level, boolean fillCache) {
        return new TwoLevelIterator<>(
            files[level].iterator(),
            (FileMetaData f) -> vset.tableCache.newIterator(f.number,f.fileSize,fillCache)
        );
        //  return NewTwoLevelIterator(
        //    new LevelFileNumIterator(vset_->icmp_, &files_[level]),
        //    &GetFileIterator, vset_->table_cache_, options);
    }

}