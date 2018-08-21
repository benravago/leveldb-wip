package lib.leveldb.db;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.OpenOption;
import static java.nio.file.StandardOpenOption.*;

import lib.leveldb.DB;
import lib.leveldb.Env;
import lib.leveldb.Slice;
import lib.leveldb.Cursor;
import static lib.leveldb.db.DbUtil.*;
import static lib.leveldb.db.DbFormat.*;

public class Factory implements lib.leveldb.Factory {

    @Override
    public DB.Builder newBuilder() {
        return new Builder();
    }

    static class Options {
        boolean createIfMissing, errorIfExists;
        boolean paranoidChecks, reuseLogs;
        int maxFileSize, maxOpenFiles, writeBufferSize;
        int blockCacheSize, blockSize, blockRestartInterval;
        DB.CompressionType compression;
        DB.FilterPolicy filterPolicy;
        DB.Comparator comparator;
        Env env;
    }

    class Builder implements DB.Builder {
        Options o = new Options();

        @Override public Builder reuseLogs(boolean b) {
            o.reuseLogs = b; return this;
        }
        @Override public Builder paranoidChecks(boolean b) {
            o.paranoidChecks = b; return this;
        }
        @Override public Builder maxFileSize(int i) {
            o.maxFileSize = i; return this;
        }
        @Override
        public Builder maxOpenFiles(int i) {
            o.maxOpenFiles = i; return this;
        }
        @Override
        public Builder writeBufferSize(int i) {
            o.writeBufferSize = i; return this;
        }
        @Override
        public Builder blockCacheSize(int i) {
            o.blockCacheSize = i; return this;
        }
        @Override
        public Builder blockSize(int i) {
            o.blockSize = i; return this;
        }
        @Override
        public Builder blockRestartInterval(int i) {
            o.blockRestartInterval = i; return this;
        }
        @Override
        public Builder comparator(DB.Comparator c) {
            o.comparator = c; return this;
        }
        @Override
        public Builder env(Env e) {
            o.env = e; return this;
        }
        @Override
        public Builder compression(DB.CompressionType c) {
            o.compression = c; return this;
        }
        @Override
        public Builder filterPolicy(DB.FilterPolicy f) {
            o.filterPolicy = f; return this;
        }

        @Override
        public DB open(Path path, OpenOption... options) {
            for (var open:options) {
                if (open == CREATE) o.createIfMissing = true;
                if (open == CREATE_NEW) o.errorIfExists = true;
            }
            DbImplBg impl = new DbImplBg();
            sanitizeOptions(impl,path,o);
            impl.open();
            return stub(impl);
        }

        @Override
        public void destroy(Path path) {
            DbUtil.destroyDB(path,o);
        }
        @Override
        public void repair(Path path) {
           DbUtil.repairDB(path,o);
        }

        @Override
        public DB.FilterPolicy newFilterPolicy() {
            return new BloomFilterPolicy(-1);
        }
        @Override
        public DB.Comparator newComparator() {
            return byteComparator();
        }
        @Override
        public Env newEnv() {
            return environment(null);
        }
    }

    static int nonZero(int value, int defaultValue) {
        return value != 0 ? value : defaultValue;
    }

    static int clipToRange(int value, int minvalue, int maxvalue) {
        return (value > maxvalue) ? maxvalue
             : (value < minvalue) ? minvalue
             :  value ;
    }

    static final int kNumNonTableCacheFiles = 10;

    static void sanitizeOptions(DbImpl db, Path path, Options src) {
        db.dbname = path; // Paths.get(dbname);
        db.env = environment(src.env);

        db.internalComparator = internalComparator(src.comparator);
        db.filterPolicy = src.filterPolicy;
        db.compression = src.compression.code;

        db.createIfMissing = src.createIfMissing;
        db.errorIfExists = src.errorIfExists;
        db.reuseLogs = src.reuseLogs;
        db.paranoidChecks = src.paranoidChecks;

        int maxOpenFiles = clipToRange(src.maxOpenFiles, 64 + kNumNonTableCacheFiles, 50000 );
        int maxFileSize = clipToRange(src.maxFileSize, 1 << 20, 1 << 30 );

        db.writeBufferSize = clipToRange(src.writeBufferSize, 64 << 10, 1 << 30 );
        db.blockSize = clipToRange(src.blockSize, 1 << 10, 4 << 20 );
        db.blockRestartInterval = nonZero(src.blockRestartInterval, 16 );

        try {
            db.infoLog = infoLog(db.dbname);
            db.infoStream = infoStream(db.dbname,db.env);
            db.infoStream.attachTo(db.infoLog);
        }
        catch (IOException e) { throw ioerror(e); }

        // Reserve ten files or so for other uses and give the rest to TableCache.
        int tableCacheSize = maxOpenFiles - kNumNonTableCacheFiles;
        int blockCacheSize = nonZero(src.blockCacheSize, 8 * 1024 * 1024 ) / src.blockSize;

        db.tableCache =
            new TableCache(db.dbname,db.env)
                .comparator(db.internalComparator)
                .filterPolicy(db.filterPolicy)
                .verifyChecksums(src.paranoidChecks)
                .cache(blockCacheSize,tableCacheSize)
                .open();

        db.versions =
            new VersionSet(db.dbname,db.env)
                .comparator(db.internalComparator)
                .paranoidChecks(src.paranoidChecks)
                .files(db.reuseLogs,maxFileSize)
                .cache(db.tableCache)
                .open();
    }

    static InternalKeyComparator internalComparator(DB.Comparator c) {
        var icmp = (c instanceof KeyComparator) ? (KeyComparator)c
                 : (c != null) ?  keyComparator(c)
                 : (KeyComparator) byteComparator();
        return new InternalKeyComparator(icmp);
    }

    static KeyComparator<Slice> keyComparator(DB.Comparator c) {
        return new KeyComparator<Slice>() {
            @Override
            public DB.Comparator comparator() {
                return c;
            }
            @Override
            public String name() {
                return c.getClass().getName();
            }
            @Override
            public int compare(Slice a, Slice b) {
                return a.equals(b) ? 0
                     : c.compare(a.data,a.offset,a.length,b.data,b.offset,b.length);
            }
        };
    }

    static DB.Comparator byteComparator() {
        return new BytewiseComparator();
    }

    static Env environment(Env env) {
        return (env != null) ? env : new FileEnv(){};
    }

    static DB stub(DbImplBg impl) {
        return new DB() {
            @Override
            public void close() {
                try { impl.close(); }
                catch (IOException e) { throw ioerror(e); }
            }
            @Override
            public Slice get(Slice key, DB.Snapshot snapshot, boolean fillCache, boolean verifyChecksums) {
                return impl.get(key,snapshot,fillCache,verifyChecksums);
            }
            @Override
            public DB.Snapshot getSnapshot() {
                return impl.getSnapshot();
            }
            @Override
            public void releaseSnapshot(DB.Snapshot snapshot) {
                impl.releaseSnapshot(snapshot);
            }
            @Override
            public DB.WriteBatch batch() {
                return impl.batch();
            }
            @Override
            public <T> T getProperty(String key) {
                return (T) DbUtil.getProperty(impl,key);
            }
            @Override
            public Cursor<Slice, Slice> iterator(DB.Snapshot snapshot, boolean fillCache, boolean verifyChecksums) {
                return impl.newIterator(snapshot,fillCache,verifyChecksums);
            }
            @Override
            public long getApproximateSize(Slice begin, Slice end) {
                return impl.getApproximateSize(begin,end);
            }
            @Override
            public void compact(Slice begin, Slice end) {
                impl.compactRange(begin,end);
            }
        };
    }
}
