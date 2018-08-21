package lib.leveldb.db;

class FileMetaData {
    int level;
    int refs = 0;
    int allowedSeeks = (1 << 30);   // Seeks allowed until compaction
    long number = 0;
    long fileSize = 0;              // File size in bytes
    DbFormat.InternalKey smallest;  // Smallest internal key served by table
    DbFormat.InternalKey largest;   // Largest internal key served by table
}
