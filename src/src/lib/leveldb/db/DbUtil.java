package lib.leveldb.db;

import java.nio.file.Path;
import java.io.IOException;

import java.lang.reflect.Array;

import java.util.Formatter;
import java.util.function.Supplier;

import lib.util.logging.Log;
import lib.util.logging.OutputHandler;

import lib.leveldb.DB;
import lib.leveldb.Env;
import lib.leveldb.Slice;
import static lib.leveldb.db.Factory.*;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.FileName.*;

import lib.leveldb.Status;
import static lib.leveldb.Status.Code.*;

interface DbUtil {

    class Ref<T> { Ref(T t){ v=t; } T v; }

    class Bool { boolean v = false; }
    class SequenceNumber { long v = 0; }

    static <T> T[] repeat(int n, Supplier<T> s) {
        T t = s.get();
        T[] a = (T[]) Array.newInstance(t.getClass(),n);
        a[0] = t;
        for (var i = 1; i < n; i++) a[i] = s.get();
        return a;
    }

    static Status notFound(String m) { return new Status(m).state(NotFound); }
    static Status corruption(String m) { return new Status(m).state(Corruption); }
    static Status notSupported(String m) { return new Status(m).state(NotSupported); }
    static Status invalidArgument(String m) { return new Status(m).state(InvalidArgument); }
    static Status ioerror(IOException e) { return new Status(e).state(IOError); }

    static Status fault(String m) { return new Status(m); }
    static Status fault(Exception e) { return new Status(e); }

    static RuntimeException check(Throwable t) {
        return (t instanceof RuntimeException) ? (RuntimeException)t : new Status(t);
    }

/*
    "key",sequenceNumber,type
    "text\015\012"
*/

    static CharSequence string(InternalKey k) {
        return new StringBuilder()
            .append('"').append(string(k.userKey))
            .append("\",").append(string(sequenceNumber(k)))
            .append(',').append(string(valueType(k)));
    }

    static CharSequence string(Slice s) {
        return string(s.data,s.length,s.offset);
    }

    static CharSequence string(byte[] b, int off, int len) {
        var s = new StringBuilder();
        var j = off + len;
        for (var i = off; i < j; i++) {
            var c = b[i] & 0x0ff;
            switch (c) {

                case '\b': s.append("\\b"); continue; // 08 BS
                case '\t': s.append("\\t"); continue; // 09 TAB
                case '\n': s.append("\\n"); continue; // 0a LF
                case '\f': s.append("\\f"); continue; // 0c FF
                case '\r': s.append("\\r"); continue; // 0d CR
                case '\"': s.append("\\\""); continue; // 22 "
                case '\\': s.append("\\\\"); continue; // 5c \

                default: {
                    if (c < 0x20 || c > 0x7e) {
                        s.append(octal(c));
                    } else {
                        s.append((char)c);
                    }
                }
            }
        }
        return s;
    }

    static CharSequence string(long l) {
        var s = Long.toHexString(l);
        var p = s.length()%2 > 0 ? "0" : "00";
        return p + s;
    }

    static char[] octal(int c) {
        var o = new char[4];
        o[0] = 0x5c;
        o[1] = (char)((c >>> 6 & 0x07) | 0x30);
        o[2] = (char)((c >>> 3 & 0x07) | 0x30);
        o[3] = (char)((c       & 0x07) | 0x30);
        return o;
    }

/*
    open:
       infoLog = DbUtil.infoLog(dbname);
       infoStream = DbUtil.infoStream(dbname,env);
       infoStream.attachTo(infoLog);

    close:
       infoStream.detachFrom(infoLog);
       infoStream.close();
*/

    static Log infoLog(Path dbname) throws IOException {
        var name = DB.class.getName() + ".log."
                 + dbname.getFileName().toString();
        return Log.getLogger(name);
    }

    static OutputHandler infoStream(Path dbname, Env env) throws IOException {
        var path = infoLogFileName(dbname);
        env.renameFile(path);
        var out = env.newAppendableFile(path);
        return new OutputHandler(out);
    }

    static void repairDB(Path path, Options o) {
        // Repair.run(name, options);
    }

    static void destroyDB(Path path, Options o) { // static void destroyDB(Options options, String name) {
        var env = environment(o.env);
        var dbname = path;
        var filenames = env.getChildren(dbname);
        if (filenames == null || filenames.length == 0) {
            // Ignore error in case directory does not exist
            return;
        }
        try {
            var lockname = lockFileName(dbname);
            var lock = env.lockFile(lockname);
            for (var filename : filenames) {
                var pfn = parseFileName(filename);
                if (pfn.type != FileType.kDBLockFile) {
                    // Lock file will be deleted at end
                    env.deleteFile(filename);
                }
            }
            env.unlockFile(lock);  // Ignore error since state is already gone
            env.deleteFile(lockname);
            env.deleteDir(dbname);  // Ignore error in case dir contains other files
        }
        catch (IOException e) { throw ioerror(e); }
    }

    static Object getProperty(DbImpl db, String property) {
      db.mutex.lock();
      try (db.mutex) {
        if (property.startsWith("leveldb.num-files-at-level")) {
            return getNumFilesAtLevel(db,Integer.parseInt(property.substring(26)));
        }
        switch (property) {
            case "leveldb.sstables":                 return getSSTables(db);
            case "leveldb.compaction-stats":         return getCompactionStats((DbImplBg)db);
            case "leveldb.approximate-memory-usage": return getApproximateMemoryUsage(db);
            case "leveldb.implementation":           return (Object)db;
            default: return null;
        }
      }
    }

    static String getNumFilesAtLevel(DbImpl db, int level) {
        return level > kNumLevels ? null
             : Integer.toString(db.versions.numLevelFiles(level));
    }

    static String getSSTables(DbImpl db) {
        return null; // TODO: Debug.string(db.versions.current());
    }

    static String getCompactionStats(DbImplBg db) {
        var f = new Formatter();
        f.format("Compactions\n")
         .format("Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n")
         .format("--------------------------------------------------\n");
        for (var level = 0; level < kNumLevels; level++) {
            var stats = db.stats[level];
            var files = db.versions.numLevelFiles(level);
            if (stats.micros > 0 || files > 0) {
                f.format("%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                         level,
                         files,
                         db.versions.numLevelBytes(level) / 1048576.0,
                         stats.micros / 1e6,
                         stats.bytesRead / 1048576.0,
                         stats.bytesWritten / 1048576.0);
            }
        }
        return f.toString();
    }

    static String getApproximateMemoryUsage(DbImpl db) {
        // size_t total_usage = options_.block_cache->TotalCharge();
        var totalUsage = db.tableCache.approximateMemoryUsage();
        if (db.memTable != null) {
            totalUsage += db.memTable.approximateMemoryUsage();
        }
        if (db.immuTable != null) {
            totalUsage += db.immuTable.approximateMemoryUsage();
        }
        return Long.toString(totalUsage);
    }
}