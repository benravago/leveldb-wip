package bsd.leveldb;

/**
 * Options that control read operations
 */
public class ReadOptions {

    /**
     * If true, all data read from underlying storage will be verified against corresponding checksums.
     * 
     * Default: false
     */
    public boolean verifyChecksums = false;

    /**
     * If true, the data read for this iteration be cached in memory.
     * Callers may wish to set this field to false for bulk scans.
     * 
     * Default: true
     */
    public boolean fillCache = true;

    /**
     * If "snapshot" is non-NULL, read as of the supplied snapshot
     * (which must belong to the DB that is being read and which must not have been released).
     * If "snapshot" is NULL, use an implicit snapshot of the state at the beginning of this read operation.
     * 
     * Default: NULL
     */
    public Snapshot snapshot = null;

}