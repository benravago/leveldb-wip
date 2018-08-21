package lib.leveldb.db;

import java.io.PrintStream;

import lib.leveldb.Slice;
import lib.leveldb.io.ByteDecoder;
import static lib.leveldb.db.Dbf.*;
import static lib.leveldb.db.VersionEdit.*;

public class Manifest {
    public static void main(String[] args) throws Exception {
        var snap = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        scan(args[0],snap,Manifest::print);
    }

    public static void print(Slice record, PrintStream out) {
        var in = new ByteDecoder().wrap(record);
        while (in.remaining() > 0) {
          var tag = in.getVarint32();
          switch (tag) {
            case kComparator: {
              out.format(" kComparator: %s\n",in.getLengthPrefixedString());
              break;
            }
            case kLogNumber: {
              out.format(" kLogNumber: %d\n",in.getVarint64());
              break;
            }
            case kPrevLogNumber: {
              out.format(" kPrevLogNumber: %d\n",in.getVarint64());
              break;
            }
            case kNextFileNumber: {
              out.format(" kNextFileNumber: %d\n",in.getVarint64());
              break;
            }
            case kLastSequence: {
              out.format(" kLastSequence: %d\n",in.getVarint64());
              break;
            }
            case kCompactPointer: {
              var level = getLevel(in);
              var key = getKey(in);
              out.format(" kCompactPointer: level=%d, key=%s\n",level,key);
              break;
            }
            case kDeletedFile: {
              var level = getLevel(in);
              var file = in.getVarint64();
              out.format(" kDeletedFile: level=%d, file=%d\n",level,file);
              break;
            }
            case kNewFile: {
              var level = getLevel(in);
              var file = in.getVarint64();
              var fileSize = in.getVarint64();
              var smallest = getKey(in);
              var largest = getKey(in);
              out.format(" kNewFile: level=%d, file=%d:%d, key=%s/%s\n",level,file,fileSize,smallest,largest);
              break;
            }
            default: {
              out.format(" unknown tag: x%02x @ %d\n",tag,in.position());
              return;
            }
          }
        }
    }

    static String getKey(ByteDecoder in) { return text(getInternalKey(in)); }

}
