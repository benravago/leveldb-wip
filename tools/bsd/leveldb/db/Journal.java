package bsd.leveldb.db;

import java.util.Map.Entry;

import bsd.leveldb.Slice;
import static bsd.leveldb.db.LogFile.*;

public class Journal {
    public static void main(String[] args) throws Exception {
        int snap = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        scan(args[0],snap,Journal::print);
    }

    static void print(Slice record) {
        Batch.Rep batch = new Batch.Rep();
        batch.setContents(record);
        for (Entry<Slice,Slice> entry:batch) {
            Slice key = entry.getKey();
            Slice value = entry.getValue();
            if (value != null) {
                System.out.println("+ "+text(key)+" = "+text(value));
            } else {
                System.out.println("- "+text(key));
            }
        }
    }
}
