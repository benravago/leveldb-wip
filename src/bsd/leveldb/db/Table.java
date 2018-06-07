package bsd.leveldb.db;

import java.nio.file.Path;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Comparator;
import java.util.Map.Entry;

import bsd.leveldb.Slice;
import bsd.leveldb.Status;
import bsd.leveldb.Cursor;
import bsd.leveldb.io.Cache;
import bsd.leveldb.io.SeekableInputStream;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.FileName.*;
import static bsd.leveldb.Status.Code.*;


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
    Entry<InternalKey,Slice> internalGet(InternalKey key, boolean fillCache);


// called from db/table_cache.cc -> TableCache::FindTable()

// include/leveldb/table.h db/table/table.cc
// Status Table::Open(const Options& options,
//                    RandomAccessFile* file,
//                    uint64_t size,
//                    Table** table)

    static Table load(
        Path dbname, Env env, long fileNumber, long fileSize,
        Comparator<InternalKey> comparator, FilterPolicy filterPolicy,
        boolean paranoidChecks, Cache<Long,Block> blockCache )
    {
        Path fname = tableFileName(dbname,fileNumber);
        if (!env.fileExists(fname)) {
            fname = sstableFileName(dbname,fileNumber);
            if (!env.fileExists(fname)) {
                return null;
            }
        }
        try {
            SeekableInputStream file = env.newRandomAccessFile(fname);
            long fileLength = file.length();
            if (fileSize != fileLength) {
                throw new Status("expected file size "+fileSize+", found "+fileLength).state(Status.Code.Corruption);
            }

            TableFile tf =
                new TableFile(file,comparator)
                    .filterPolicy(filterPolicy)
                    .verifyChecksums(paranoidChecks)
                    .cache(blockCache,(int)fileNumber)
                    .open();

            return tf;
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }
    }

// called from db/db_impl.cc -> DBImpl::WriteLevel0Table()

// db/builder.{h,cc}
// Status BuildTable(const std::string& dbname,
//                   Env* env,
//                   const Options& options,
//                   TableCache* table_cache,
//                   Iterator* iter,
//                   FileMetaData* meta)

    /**
     * Build a Table file from the contents of *iter.
     * The generated file will be named according to meta->number.
     * On success, the rest of *meta will be filled with metadata about the generated table.
     * If no data is present in *iter, meta->file_size will be set to zero,
     * and no Table file will be produced.
     */
    static FileMetaData store(
        Path dbname, Env env, long fileNumber,
        KeyComparator<InternalKey> comparator, FilterPolicy filterPolicy,
        int blockSize, int blockRestartInterval, int compressionType,
        Cursor<InternalKey,Slice> iter, TableCache tableCache )
    {
        FileMetaData meta = new FileMetaData();
        meta.number = fileNumber;
        meta.fileSize = 0;
        if (!iter.hasNext()) {
            return meta;
        }
        Path fname = tableFileName(dbname, meta.number);
        try (OutputStream file = env.newWritableFile(fname)) {

            TableBuilder builder =
                new TableBuilder(file,comparator)
                    .block(blockSize,blockRestartInterval)
                    .filterPolicy(filterPolicy)
                    .compression(compressionType);

            InternalKey key = iter.next().getKey();
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
            file.close();
        }
        catch (IOException e) {
            throw new Status(e).state(IOError);
        }
        return meta;

        // if (s.ok()) {
        //   // Verify that the table is usable
        //   Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
        //   s = it->status();
        //   delete it;
        // }
        // // Check for input iterator errors
        // if (!iter->status().ok()) {
        //   s = iter->status();
        // }
        // if (s.ok() && meta->file_size > 0) {
        //   // Keep it
        // } else {
        //   env->DeleteFile(fname);
        // }
    }

}
