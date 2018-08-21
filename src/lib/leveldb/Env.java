package lib.leveldb;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.channels.FileLock;

import java.util.concurrent.Future;

import lib.io.SeekableInputStream;

/**
 * An Env is an interface used by the leveldb implementation
 * to access operating system functionality like the filesystem etc.
 *
 * Callers may wish to provide a custom Env object when opening a database
 * to get fine-grained control; e.g., to rate limit file system operations.
 * <p>
 * All Env implementations should be safe for concurrent access
 * from multiple threads without any external synchronization.
 */
public interface Env {

    /**
     * Create a brand new sequentially-readable file with the specified name.
     * The returned file should only be accessed by one thread at a time.
     *
     * @param  fname  the path to the file to open
     * @return a new input stream
     * @throws IOException - if an I/O error occurs
     */
    InputStream newSequentialFile(Path fname) throws IOException;

    /**
     * Create a brand new random access read-only file with the specified name.
     * The returned file should only be accessed by one thread at a time.
     *
     * @param  fname  the path to the file to open
     * @return a new input stream
     * @throws IOException - if an I/O error occurs
     */
    SeekableInputStream newRandomAccessFile(Path fname) throws IOException;

    /**
     * Create an object that writes to a new file with the specified name.
     * Deletes any existing file with the same name and creates a new file.
     * The returned file should only be accessed by one thread at a time.
     *
     * @param  fname  the path to the file to open
     * @return a new output stream
     * @throws IOException - if an I/O error occurs
     */
    OutputStream newWritableFile(Path fname) throws IOException;

    /**
     * Create an object that either appends to an existing file,
     * or writes to a new file (if the file does not exist to begin with).
     * The returned file should only be accessed by one thread at a time.
     *
     * @param  fname  the path to the file to open
     * @return a new output stream
     * @throws IOException - if an I/O error occurs
     */
    OutputStream newAppendableFile(Path fname) throws IOException;

    /**
     * Checks if a named file exists.
     *
     * @param  fname  the path to the file to check
     * @return true iff the named file exists.
     */
    boolean fileExists(Path fname);

    /**
     * Return the names of the children of the specified directory.
     * The names are relative to "dir".
     *
     * @param  dir  the path to the directory
     * @return a list with the entries in the directory.
     */
    Path[] getChildren(Path dir);

    /**
     * Rename the named file with a generated suffix.
     *
     * @param  fname  the path to the file to check
     * @return true iff the file was renamed.
     * @throws IOException - if an I/O error occurs
     */
    boolean renameFile(Path fname) throws IOException;

    /**
     * Delete the named file.
     *
     * @param  fname  the path to the directory
     * @throws IOException - if an I/O error occurs
     */
    void deleteFile(Path fname) throws IOException;

    /**
     * Create the specified directory.
     *
     * @param  dirname  the path to the directory
     * @throws IOException - if an I/O error occurs
     */
    void createDir(Path dirname) throws IOException;

    /**
     * Delete the specified directory.
     * @param  dirname  the path to the directory
     * @throws IOException - if an I/O error occurs
     */
    void deleteDir(Path dirname) throws IOException;

    /**
     * Return the size of the named file.
     *
     * @param  fname  the path to the file to check
     * @return the file size;
     * @throws IOException - if an I/O error occurs
     */
    long getFileSize(Path fname) throws IOException;

    /**
     * Rename file src to target.
     *
     * @param  src  the path to the file to move (or rename)
     * @param  target  the path to the target file
     * @throws IOException - if an I/O error occurs
     */
    void renameFile(Path src, Path target) throws IOException;

    /**
     * Lock the specified file.
     *
     * Used to prevent concurrent access to the same db by multiple processes.
     * <p>
     * The caller should call unlockFile(lock) to release the lock.
     * If the process exits, the lock will be automatically released.
     * <p>
     * If somebody else already holds the lock, finishes immediately and returns null.
     * I.e., this call does not wait for existing locks to go away.
     * <p>
     * May create the named file if it does not already exist.
     *
     * @param  fname  the path to the resource to lock
     * @return a lock object representing the newly-acquired lock,
     *         or null if named resource is already locked
     * @throws IOException - if an I/O error occurs
     */
    FileLock lockFile(Path fname) throws IOException;

    /**
     * Release the lock acquired by a previous successful call to lockFile().
     *
     * @param  lock  a lock which was returned by a successful lockFile() call
     * @throws IOException - if an I/O error occurs
     */
    void unlockFile(FileLock lock) throws IOException;

    /**
     * If the operation is supported, Force all system buffers to synchronize with the underlying device.
     *
     * @param out  the OutputStream to sync
     * @throws IOException - if an I/O error occurs
     */
    void syncFile(OutputStream out) throws IOException;

    /**
     * A utility routine: write "data" to the named file with a file sync.
     *
     * @param fname
     * @param data bytes
     * @throws IOException
     */
    void writeToFile(Path fname, byte[] data) throws IOException;

    /**
     * A utility routine: read contents of named file into *data.
     *
     * @param fname
     * @return data bytes
     * @throws java.io.IOException
     */
    byte[] readFromFile(Path fname) throws IOException;

    /**
     * Arrange to run a Runnable function once in a background thread.
     *
     * "function" may run in an unspecified thread.
     * Multiple functions added to the same Env may run concurrently in different threads.
     * I.e., the caller may not assume that background work items are serialized.
     *
     * @param  task  the function to be run
     * @return a Future that can be used to track the task
     */
    Future<?> schedule(Runnable task);

    /**
     * Returns the number of micro-seconds since some fixed point in time.
     * Only useful for computing deltas of time.
     *
     * @return the current time in microseconds
     */
    long nowMicros();

    /**
     * Sleep/delay the thread for the prescribed number of micro-seconds.
     *
     * @param  micros  the length of time to sleep in microseconds
     */
    void sleepForMicroseconds(long micros);
}