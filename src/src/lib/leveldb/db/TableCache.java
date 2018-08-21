package lib.leveldb.db;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Map.Entry;

import lib.util.LruMap;
import lib.util.LruHashMap;

import lib.leveldb.Env;
import lib.leveldb.Slice;
import lib.leveldb.Cursor;
import lib.leveldb.DB.FilterPolicy;
import static lib.leveldb.db.DbFormat.*;

/**
 *  Note: should be "Thread-safe (provides internal synchronization)".
 */
class TableCache implements Closeable {

    Env env;
    Path dbname;
    //   const Options* options_;
    int blockCacheSize;
    boolean verifyChecksums;
    KeyComparator<InternalKey> comparator;
    FilterPolicy filterPolicy;

    LruMap<Long,Table> cache;
    LruMap<Long,Block> blockCache;

    TableCache(Path dbname, Env env) {
        this.dbname = dbname;
        this.env = env;
    }
    TableCache comparator(KeyComparator<InternalKey> icmp) {
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
    Entry<InternalKey,Slice> get(
        long fileNumber, long fileSize, InternalKey ikey, boolean fillCache )
    {
        //   Cache::Handle* handle = NULL;
        var t = findTable(fileNumber,fileSize);
        if (t != null) { // if (s.ok()) {
//     Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
//     s = t->InternalGet(options, k, arg, saver);
//     cache_->Release(handle);
            return t.internalGet(ikey,fillCache); // ,verifyChecksums);
        }
        return null; // return s;
    }

    Table findTable(long fileNumber, long fileSize) {
        var table = cache.get(fileNumber); // *handle = cache_->Lookup(key);
        if (table == null) { // if (*handle == NULL) {
            table = Table.load( dbname, fileNumber, fileSize,
                                env, comparator, filterPolicy,
                                blockCache, verifyChecksums );
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
        var table = findTable(fileNumber,fileSize);
        if (table == null) {
            return null; // return NewErrorIterator(s);
        }
        var result = table.newIterator(fillCache);
        // result->RegisterCleanup(&UnrefEntry, cache_, handle);
        return result;
    }

    long approximateMemoryUsage() {
        var estimate = 0L;
//      for (Block block : blockCache.values()) {
//          estimate += block.contents.length;
//      }
//      for (Table t : cache.values()) {
//          TableFile table = (TableFile)t;
//          estimate += table.indexData.length;
//          if (table.filterData != null) {
//              estimate += table.filterData.length;
//          }
//      }
        return estimate;
    }

}