package lib.leveldb.db;

import java.io.PrintStream;
import java.nio.file.Paths;
import java.nio.file.Files;

import java.util.function.BiConsumer;

import lib.leveldb.Slice;
import lib.leveldb.io.Escape;
import lib.leveldb.io.ByteArray;

public class Dbf {
    public static void main(String[] args) throws Exception {
        var fn = filename(args);
        if (fn.contains("MANIFEST-")) Manifest.main(args);
        else if (fn.endsWith(".log")) Journal.main(args);
        else if (fn.endsWith(".ldb")) SSTable.main(args);
    }

    static String filename(String[] args) {
        if (args.length < 1) {
            System.out.println("usage: java Dbf [filename]");
            System.exit(1);
        }
        return args[0];
    }

    static byte[] bytes(String fn) throws Exception {
        return Files.readAllBytes(Paths.get(fn));
    }

    static void scan(String fn, int v, BiConsumer<Slice,PrintStream> action) throws Exception {
        var out = (v == 1) ? System.out : (v == 2) ? System.err : null;
        var in = new ByteArray(bytes(fn));
        var rdr = new LogReader(in);
        rdr.forEach(record -> {
            var begin = in.mark();
            if (out != null) in.snap(out,rdr.offset);
            in.mark(0);
            assert(in.mark() == rdr.offset);
            System.out.format("hdr: %s @ x%02x\n", header(in.array(),begin), begin );
            action.accept(record,System.out);
        });
    }

    static String header(byte[] b, int o) {
        var l = 0x80L;
        for (var i = 6; i >= 0; i--) {
            l = (l << 8) | (b[o+i] & 0x0ff);
        }
        var x = Long.toHexString(l);
        return x.substring(2,4)+','+x.substring(4,8)+','+x.substring(8);
    }

    static String text(Slice s) {
        return "\""+Escape.chars(s.data,s.offset,s.length)+"\"";
    }

    static String text(DbFormat.InternalKey key) {
        return text(key.userKey);
    }
}
