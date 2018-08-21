package lib.leveldb.db;

import java.util.Map;

import java.io.Closeable;
import java.nio.file.Path;
import java.io.IOException;

import lib.leveldb.Env;
import lib.leveldb.Slice;
import lib.leveldb.Status;
import lib.leveldb.Cursor;
import lib.leveldb.DB.FilterPolicy;
import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.FileName.*;
import static lib.leveldb.Status.Code.*;

/**
 * A Table is a sorted map from strings to strings.
 *
 * Tables are immutable and persistent.
 * A Table may be safely accessed from multiple threads without external synchronization.
 */
interface Table extends Closeable {

    // Returns a new iterator over the table contents.
    // The result of NewIterator() is initially invalid
    // (caller must call one of the Seek methods on the iterator before using it).
    Cursor<InternalKey,Slice> newIterator(boolean fillCache);

    // Calls (*handle_result)(arg, ...) with the entry found after a call to Seek(key).
    // May not make such a call if filter policy says that key is not present.
    Map.Entry<InternalKey,Slice> internalGet(InternalKey key, boolean fillCache);

    static Slice NotFound = new Slice(null,0,0);

    static Table load(
            Path dbname, long fileNumber, long fileSize,
            Env env, KeyComparator<InternalKey> comparator, FilterPolicy filterPolicy,
            Map<Long,Block> blockCache, boolean paranoidChecks )
    {
        var fname = tableFileName(dbname,fileNumber);
        if (!env.fileExists(fname)) {
            fname = sstableFileName(dbname,fileNumber);
            if (!env.fileExists(fname)) {
                return null;
            }
        }
        try {
            var file = env.newRandomAccessFile(fname);
            var fileLength = file.length();
            if (fileSize != fileLength) {
                throw new Status("expected file size "+fileSize+", found "+fileLength).state(Corruption);
            }

            var table =
                new TableFile(file,comparator)
                    .filterPolicy(filterPolicy)
                    .verifyChecksums(paranoidChecks)
                    .cache(blockCache,(int)fileNumber)
                    .open();

            return table;
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }
    }

    /**
     * Build a Table file from the contents of *iter.
     * The generated file will be named according to meta->number.
     * On success, the rest of *meta will be filled with metadata about the generated table.
     * If no data is present in *iter, meta->file_size will be set to zero,
     * and no Table file will be produced.
     */
    static FileMetaData store(
            Path dbname, long fileNumber, int blockSize, int blockRestartInterval,
            Env env, KeyComparator<InternalKey> comparator, FilterPolicy filterPolicy,
            Cursor<InternalKey,Slice> iter, int compressionType )
    {
        var meta = new FileMetaData();
        meta.number = fileNumber;
        meta.fileSize = 0;
        if (!iter.hasNext()) {
            return meta;
        }
        var fname = tableFileName(dbname, meta.number);
        try (var file = env.newWritableFile(fname)) {

            var builder =
                new TableBuilder(file,comparator)
                    .block(blockSize,blockRestartInterval)
                    .filterPolicy(filterPolicy)
                    .compression(compressionType);

            var key = iter.next().getKey();
            meta.smallest = key;
            for (;;) {
                builder.add(key,iter.getValue());
                if (!iter.hasNext()) break;
                key = iter.next().getKey();
            }
            meta.largest = key;

            // Finish and check for builder errors
            builder.finish();
            meta.fileSize = builder.fileSize();
            assert (meta.fileSize > 0);

            // Finish and check for file errors
            file.flush();
            env.syncFile(file);
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }
        return meta;
    }

}

// called from db/table_cache.cc -> TableCache::FindTable()

// include/leveldb/table.h db/table/table.cc
// Status Table::Open(const Options& options,  -> Table.load()
//                    RandomAccessFile* file,
//                    uint64_t size,
//                    Table** table)

// called from db/db_impl.cc -> DBImpl::WriteLevel0Table()

// db/builder.{h,cc}
// Status BuildTable(const std::string& dbname,  -> Table.store()
//                   Env* env,
//                   const Options& options,
//                   TableCache* table_cache,
//                   Iterator* iter,
//                   FileMetaData* meta)
