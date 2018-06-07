package bsd.leveldb.db;

import java.util.Map;
import java.util.Comparator;

import java.io.Closeable;
import java.nio.file.Path;

import bsd.leveldb.Slice;
import bsd.leveldb.Cursor;
import bsd.leveldb.io.Cache;
import bsd.leveldb.io.LruHashMap;
import static bsd.leveldb.db.DbFormat.*;

/**
 *  Note: should be "Thread-safe (provides internal synchronization)".
 */
class TableCache implements Closeable {

    Env env;
    Path dbname;
    //   const Options* options_;
    int blockCacheSize;
    boolean verifyChecksums;
    Comparator<InternalKey> comparator;
    FilterPolicy filterPolicy;

    Cache<Long,Table> cache;
    Cache<Long,Block> blockCache;

    TableCache(Path dbname, Env env) {
        this.dbname = dbname;
        this.env = env;
    }
    TableCache comparator(Comparator<InternalKey> icmp) {
        comparator = icmp; return this;
    }
    TableCache filterPolicy(FilterPolicy policy) {
        filterPolicy = policy; return this;
    }
    TableCache verifyChecksums(boolean check) {
        verifyChecksums = check; return this;
    }
    TableCache cache(int blockCacheSize, int tableCacheSize ) {
        blockCache = new LruHashMap<>(blockCacheSize);
        cache = new LruHashMap<>(tableCacheSize);
        return this;
    }

    TableCache open() {
        return this;
    }

    @Override
    public void close() { // TableCache::~TableCache()
        // delete cache_;
    }

    // If a seek to internal key "k" in specified file finds an entry,
    // call (*handle_result)(arg, found_key, found_value).
    Map.Entry<InternalKey,Slice> get( long fileNumber, long fileSize,
        InternalKey ikey, boolean fillCache )
    {
        //   Cache::Handle* handle = NULL;
        Table t = findTable(fileNumber,fileSize);
        if (t != null) { // if (s.ok()) {
//     Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
//     s = t->InternalGet(options, k, arg, saver);
//     cache_->Release(handle);
            return t.internalGet(ikey,fillCache); // ,verifyChecksums);
        }
        return null; // return s;
    }

    Table findTable(long fileNumber, long fileSize) {
        Table table = cache.get(fileNumber); // *handle = cache_->Lookup(key);
        if (table == null) { // if (*handle == NULL) {
            table = Table.load( dbname, env, fileNumber, fileSize,
                                comparator, filterPolicy, verifyChecksums, blockCache );
            //  s = Table::Open(*options_, file, file_size, &table);
            if (table == null) { // if (!s.ok()) {
                // assert(table == NULL);
                // delete file;
                // We do not cache error results so that if the error is transient,
                // or somebody repairs the file, we recover automatically.
            } else {
                cache.put(fileNumber,table);
                // TableAndFile* tf = new TableAndFile;
                // tf->file = file;
                // tf->table = table;
                // *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
            }
        }
        return table;
    }

    /**
     * Evict any entry for the specified file number.
     */
    void evict(long fileNumber) {
        cache.remove(fileNumber);
        // TODO: also evict blockCache of related blocks
    }

    /**
     * Return an iterator for the specified file number
     * (the corresponding file length must be exactly "file_size" bytes).
     * If "tableptr" is non-NULL, also sets "*tableptr" to point to
     * the Table object underlying the returned iterator,
     * or NULL if no Table object underlies the returned iterator.
     * The returned "*tableptr" object is owned by the cache and should not be deleted,
     * and is valid for as long as the returned iterator is live.
     */
    Cursor<InternalKey,Slice> newIterator(long fileNumber, long fileSize, boolean fillCache) {
        Table table = findTable(fileNumber,fileSize);
        if (table == null) {
            return null; // return NewErrorIterator(s);
        }
        Cursor<InternalKey,Slice> result = table.newIterator(fillCache);
        // result->RegisterCleanup(&UnrefEntry, cache_, handle);
        return result;
    }

    long approximateMemoryUsage() {
        long estimate = 0;
        for (Block block : blockCache.values()) {
            estimate += block.contents.length;
        }
        for (Table t : cache.values()) {
            TableFile table = (TableFile)t;
            estimate += table.indexData.length;
            if (table.filterData != null) {
                estimate += table.filterData.length;
            }
        }
        return estimate;
    }

}



// #endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
// // Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// // Use of this source code is governed by a BSD-style license that can be
// // found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// #include "db/table_cache.h"
//
// #include "db/filename.h"
// #include "leveldb/env.h"
// #include "leveldb/table.h"
// #include "util/coding.h"
//
// namespace leveldb {
//
// struct TableAndFile {
//   RandomAccessFile* file;
//   Table* table;
// };
//
// static void DeleteEntry(const Slice& key, void* value) {
//   TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
//   delete tf->table;
//   delete tf->file;
//   delete tf;
// }
//
// static void UnrefEntry(void* arg1, void* arg2) {
//   Cache* cache = reinterpret_cast<Cache*>(arg1);
//   Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
//   cache->Release(h);
// }


