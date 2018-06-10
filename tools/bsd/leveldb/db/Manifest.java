package bsd.leveldb.db;

import bsd.leveldb.Slice;
import bsd.leveldb.io.ByteDecoder;
import static bsd.leveldb.db.Dbf.*;
import static bsd.leveldb.db.VersionEdit.*;

public class Manifest {
    public static void main(String[] args) throws Exception {
        int snap = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        scan(args[0],snap,Manifest::print);
    }

    static void print(Slice record) {
        ByteDecoder in = new ByteDecoder().wrap(record);
        while (in.remaining() > 0) {
          int tag = in.getVarint32();
          switch (tag) {
            case kComparator: {
              System.out.println(" kComparator: "+in.getLengthPrefixedString());
              break;
            }
            case kLogNumber: {
              System.out.println(" kLogNumber: "+in.getVarint64());
              break;
            }
            case kPrevLogNumber: {
              System.out.println(" kPrevLogNumber: "+in.getVarint64());
              break;
            }
            case kNextFileNumber: {
              System.out.println(" kNextFileNumber: "+in.getVarint64());
              break;
            }
            case kLastSequence: {
              System.out.println(" kLastSequence: "+in.getVarint64());
              break;
            }
            case kCompactPointer: {
              int level = getLevel(in);
              String key = getKey(in);
              System.out.println(" kCompactPointer: level="+level+", key="+key);
              break;
            }
            case kDeletedFile: {
              int level = getLevel(in);
              long file = in.getVarint64();
              System.out.println(" kDeletedFile: level="+level+", file="+file);
              break;
            }
            case kNewFile: {
              int level = getLevel(in);
              long file = in.getVarint64();
              long fileSize = in.getVarint64();
              String smallest = getKey(in);
              String largest = getKey(in);
              System.out.println(" kNewFile: level="+level+", file="+file+':'+fileSize+", key="+smallest+'/'+largest);
              break;
            }
            default: {
              System.out.println(" unknown tag: 0"+Integer.toHexString(tag)+" @ "+in.position());
              return;
            }
          }
        }
    }

    static String getKey(ByteDecoder in) { return text(getInternalKey(in)); }

}
