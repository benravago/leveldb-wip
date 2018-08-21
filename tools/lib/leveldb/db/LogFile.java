package bsd.leveldb.db;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.PrintStream;

import java.util.function.Consumer;

import bsd.leveldb.Slice;
import bsd.leveldb.io.Escape;
import bsd.leveldb.io.ByteArray;
import bsd.leveldb.db.DbFormat.InternalKey;

class LogFile {

    static byte[] bytes(String fn) throws Exception {
        return Files.readAllBytes(Paths.get(fn));
    }

    static void scan(String fn, int v, Consumer<Slice> action) throws Exception {
        PrintStream out = (v == 1) ? System.out : (v == 2) ? System.err : null;
        ByteArray in = new ByteArray(bytes(fn));
        LogReader rdr = new LogReader(in);
        rdr.forEach((record) -> {
            int begin = in.mark();
            in.snap(out,rdr.offset);
            in.mark(0);
            assert(in.mark() == rdr.offset);
            System.out.println("hdr: "+header(in.array(),begin)+" @ 0"+Integer.toHexString(begin));
            action.accept(record);
        });
    }

    static String header(byte[] b, int o) {
        long l = 0x80;
        for (int i = 6; i >= 0; i--) {
            l = (l << 8) | (b[o+i] & 0x0ff);
        }
        String x = Long.toHexString(l);
        return x.substring(2,4)+','+x.substring(4,8)+','+x.substring(8);
    }

    static String text(Slice s) {
        return "\""+Escape.chars(s.data,s.offset,s.length)+"\"";
    }

    static String text(InternalKey key) {
        return text(key.userKey);
    }
}
