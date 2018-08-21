package lib.leveldb.db;

import java.io.PrintStream;

import lib.leveldb.Slice;
import static lib.leveldb.db.Dbf.*;

public class Journal {
    public static void main(String[] args) throws Exception {
        var snap = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        scan(args[0],snap,Journal::print);
    }

    public static void print(Slice record, PrintStream out) {
        var batch = new Batch.Read();
        batch.setContents(record);
        for (var entry:batch) {
            var key = entry.getKey();
            var value = entry.getValue();
            if (value != null) {
                out.format("+ %s = %s\n",text(key),text(value));
            } else {
                out.format("- %s\n",text(key));
            }
        }
    }
}
