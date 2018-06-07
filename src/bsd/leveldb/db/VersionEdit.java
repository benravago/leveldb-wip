package bsd.leveldb.db;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import bsd.leveldb.Slice;
import bsd.leveldb.Status;
import bsd.leveldb.io.ByteDecoder;
import bsd.leveldb.io.ByteEncoder;
import static bsd.leveldb.db.DbFormat.*;

class VersionEdit {

    // Tag numbers for serialized VersionEdit.
    // These numbers are written to disk and should not be changed.
    static final int kComparator = 1;
    static final int kLogNumber = 2;
    static final int kNextFileNumber = 3;
    static final int kLastSequence = 4;
    static final int kCompactPointer = 5;
    static final int kDeletedFile = 6;
    static final int kNewFile = 7;
    // 8 was used for large value refs
    static final int kPrevLogNumber = 9;

    String comparator;
    long logNumber;
    long prevLogNumber;
    long nextFileNumber;
    long lastSequence;

    boolean hasComparator;
    boolean hasLogNumber;
    boolean hasPrevLogNumber;
    boolean hasNextFileNumber;
    boolean hasLastSequence;

    // std::vector< std::pair<int, InternalKey> > compact_pointers_;
    List<Object[]> compactPointers = new ArrayList<>();

    // std::vector< std::pair<int, FileMetaData> > new_files_;
    List<Object[]> newFiles = new ArrayList<>();

    static int first = 0;
    static int second = 1;

    // typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;
    // DeletedFileSet deleted_files_;
    Set<String> deletedFiles = new HashSet<>();

    static String deletedFile(int level, long file) {
        return ""+level+"_"+file;
    }
    static long[] deletedFile(String level_file) {
        int p = level_file.indexOf("_");
        return new long[]{Long.parseLong(level_file.substring(0,p)),
                          Long.parseLong(level_file.substring(p+1))};
    }


    void clear() {
        comparator = null;
        logNumber = 0;
        prevLogNumber = 0;
        lastSequence = 0;
        nextFileNumber = 0;
        hasComparator = false;
        hasLogNumber = false;
        hasPrevLogNumber = false;
        hasNextFileNumber = false;
        hasLastSequence = false;
        deletedFiles.clear();
        newFiles.clear();
        compactPointers.clear();
    }

    void setComparatorName(String name) {
        hasComparator = true;
        comparator = name;
    }
    void setLogNumber(long num) {
        hasLogNumber = true;
        logNumber = num;
    }
    void setPrevLogNumber(long num) {
        hasPrevLogNumber = true;
        prevLogNumber = num;
    }
    void setNextFile(long num) {
        hasNextFileNumber = true;
        nextFileNumber = num;
    }
    void setLastSequence(long seq) {
        hasLastSequence = true;
        lastSequence = seq;
    }
    void setCompactPointer(Integer level, InternalKey key) {
        compactPointers.add(new Object[]{level,key});
    }

    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    void addFile(int level, long file, long fileSize, InternalKey smallest, InternalKey largest) {
        FileMetaData f = new FileMetaData();
        f.level = level;
        f.number = file;
        f.fileSize = fileSize;
        f.smallest = smallest;
        f.largest = largest;
        newFiles.add(new Object[]{level,f});
    }

    // Delete the specified "file" from the specified "level".
    void deleteFile(int level, long file) {
        deletedFiles.add(deletedFile(level,file));
    }

    byte[] encodeTo() {
        ByteEncoder dst = new ByteEncoder();

        if (hasComparator) {
            dst.putVarint32(kComparator);
            dst.putLengthPrefixedString(comparator);
        }
        if (hasLogNumber) {
            dst.putVarint32(kLogNumber);
            dst.putVarint64(logNumber);
        }
        if (hasPrevLogNumber) {
            dst.putVarint32(kPrevLogNumber);
            dst.putVarint64(prevLogNumber);
        }
        if (hasNextFileNumber) {
            dst.putVarint32(kNextFileNumber);
            dst.putVarint64(nextFileNumber);
        }
        if (hasLastSequence) {
            dst.putVarint32(kLastSequence);
            dst.putVarint64(lastSequence);
        }

        for (int i = 0; i < compactPointers.size(); i++) {
            Object[] compactPointer = compactPointers.get(i);
            dst.putVarint32(kCompactPointer);
            dst.putVarint32((int)compactPointer[first]);   // level
            dst.putLengthPrefixedSlice(encodeInternalKey((InternalKey)compactPointer[second]));
        }

        for (String iter : deletedFiles) {
            long[] deletedFile = deletedFile(iter);
            dst.putVarint32(kDeletedFile);
            dst.putVarint32((int)deletedFile[first]);    // level
            dst.putVarint64((long)deletedFile[second]);  // file number
        }

        for (int i = 0; i < newFiles.size(); i++) {
            Object[] newFile = newFiles.get(i);
            FileMetaData f = (FileMetaData)newFile[second];
            dst.putVarint32(kNewFile);
            dst.putVarint32((int)newFile[first]);  // level
            dst.putVarint64(f.number);
            dst.putVarint64(f.fileSize);
            dst.putLengthPrefixedSlice(encodeInternalKey(f.smallest));
            dst.putLengthPrefixedSlice(encodeInternalKey(f.largest));
        }

        return dst.toByteArray();
    }

    void decodeFrom(Slice s) { decodeFrom(s.data,s.offset,s.length); }

    void decodeFrom(byte[] b, int off, int len) {
        clear();
        ByteDecoder input = new ByteDecoder().wrap(b,off,len);
        while (input.remaining() > 0) {
          int tag = input.getVarint32();
          switch (tag) {
            case kComparator: {
              setComparatorName(input.getLengthPrefixedString());
              break;
            }
            case kLogNumber: {
              setLogNumber(input.getVarint64());
              break;
            }
            case kPrevLogNumber: {
              setPrevLogNumber(input.getVarint64());
              break;
            }
            case kNextFileNumber: {
              setNextFile(input.getVarint64());
              break;
            }
            case kLastSequence: {
              setLastSequence(input.getVarint64());
              break;
            }
            case kCompactPointer: {
              setCompactPointer(
                getLevel(input), // level
                getInternalKey(input) // key
              );
              break;
            }
            case kDeletedFile: {
              deleteFile(
                getLevel(input), // level
                input.getVarint64() // file
              );
              break;
            }
            case kNewFile: {
              addFile(
                getLevel(input), // level
                input.getVarint64(), // file
                input.getVarint64(), // fileSize
                getInternalKey(input), // smallest
                getInternalKey(input) // largest
              );
              break;
            }
            default: throw new Status("unknown tag: "+tag).state(Status.Code.Corruption);
          }
        }
    }

    static InternalKey getInternalKey(ByteDecoder input) {
        return decodeInternalKey(input.getLengthPrefixedSlice());
    }

    static int getLevel(ByteDecoder input) {
        int v = input.getVarint32();
        assert (v < DbFormat.kNumLevels);
        return v;
    }

}
