package bsd.leveldb.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;

import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import java.util.stream.Stream;

import bsd.leveldb.io.SeekableInputStream;
import bsd.leveldb.io.SeekableFileInputStream;

/**
 * An Env is an interface used by the leveldb implementation
 * to access operating system functionality like the filesystem, etc.
 *
 * Callers may wish to provide a custom Env object when opening a database
 * to get fine gain control; e.g., to rate limit file system operations.
 *
 * All Env implementations are safe for concurrent access from
 * multiple threads without any external synchronization.
 */
public interface Env {

    /***
     * Create a brand new sequentially-readable file with the specified name.
     *
     * On success, stores a pointer to the new file in *result and returns OK.
     * On failure stores NULL in *result and returns non-OK.  If the file does
     * not exist, returns a non-OK status.  Implementations should return a
     * NotFound status when the file does not exist.
     *
     * The returned file will only be accessed by one thread at a time.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default InputStream newSequentialFile(Path fname) throws IOException {
        return Files.newInputStream(fname, StandardOpenOption.READ );
    }

    /**
     * Create a brand new random access read-only file with the specified name.
     *
     * On success, stores a pointer to the new file in *result and returns OK.
     * On failure stores NULL in *result and returns non-OK.
     * If the file does not exist, returns a non-OK status.
     * Implementations should return a NotFound status when the file does not exist.
     *
     * The returned file may be concurrently accessed by multiple threads.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default SeekableInputStream newRandomAccessFile(Path fname) throws IOException {
        return new SeekableFileInputStream(fname);
    }

    /**
     * Create an object that writes to a new file with the specified name.
     *
     * Deletes any existing file with the same name and creates a new file.
     * On success, stores a pointer to the new file in *result and returns OK.
     * On failure stores NULL in *result and returns non-OK.
     *
     * The returned file will only be accessed by one thread at a time.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default OutputStream newWritableFile(Path fname) throws IOException {
        return Files.newOutputStream(fname,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING );
    }

    /**
     * Create an object that either appends to an existing file,
     * or writes to a new file (if the file does not exist to begin with).
     *
     * On success, stores a pointer to the new file in *result and returns OK.
     * On failure stores NULL in *result and returns non-OK.
     *
     * The returned file will only be accessed by one thread at a time.
     *
     * May return an IsNotSupportedError error if this Env does not allow appending to an existing file.
     * Users of Env (including the leveldb implementation) must be prepared to deal with an Env that does not support appending.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default OutputStream newAppendableFile(Path fname) throws IOException {
        return Files.newOutputStream(fname,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND );
    }

    /**
     * Returns true iff the named file exists.
     *
     * @param fname
     * @return
     */
    default boolean fileExists(Path fname) {
        return Files.exists(fname);
    }

    /**
     * Store in *result the names of the children of the specified directory.
     *
     * The names are relative to "dir".
     *
     * @param fname
     * @return
     */
    default Path[] getChildren(Path fname) {
        if (Files.isDirectory(fname)) {
            try (Stream<Path> list = Files.list(fname)) {
                return list.toArray(n -> new Path[n]);
            }
            catch (IOException ignore) {
                return null;
            }
        }
        return new Path[0];
    }

    /**
     * Delete the named file.
     *
     * @param fname
     * @throws java.io.IOException
     */
    default void deleteFile(Path fname) throws IOException {
        Files.deleteIfExists(fname);
    }

    /**
     * Create the specified directory.
     *
     * @param dirname
     * @return
     * @throws java.io.IOException
     */
    default Path createDir(Path dirname) throws IOException {
        return fileExists(dirname) ? dirname
             : Files.createDirectories(dirname,fileAttribute("rwxr-xr-x"));
    }
    static FileAttribute<?> fileAttribute(String permissions) {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permissions));
    }

    /**
     * Delete the specified directory.
     *
     * @param dirname
     * @throws java.io.IOException
     */
    default void deleteDir(Path dirname) throws IOException {
        for (Path filename : getChildren(dirname)) Files.delete(filename);
        Files.delete(dirname);
    }

    /**
     * Store the size of fname in *file_size.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default long getFileSize(Path fname) throws IOException {
        return Files.size(fname);
    }

    /**
     * Rename file src to target.
     *
     * @param src
     * @param target
     * @return
     * @throws java.io.IOException
     */
    default Path renameFile(Path src, Path target) throws IOException {
        return fileExists(src) ? Files.move(src,target,StandardCopyOption.REPLACE_EXISTING) : src;
    }

    default Path renameFile(Path src) throws IOException {
        if (!fileExists(src)) return src;
        String ft = Files.getLastModifiedTime(src).toInstant().toString();
        Path target = src.getParent().resolve(src.getFileName().toString()+'-'+ft);
        return Files.move(src,target,StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Lock the specified file.
     *
     * Used to prevent concurrent access to the same db by multiple processes.
     * On failure, stores NULL in lock and returns non-OK.
     *
     * On success, stores a pointer to the object that represents the acquired lock in *lock and returns OK.
     * The caller should call UnlockFile(*lock) to release the lock.
     * If the process exits, the lock will be automatically released.
     *
     * If somebody else already holds the lock, finishes immediately with a failure.
     * I.e., this call does not wait for existing locks to go away.
     *
     * May create the named file if it does not already exist.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default FileLock lockFile(Path fname) throws IOException {
        return FileChannel.open(fname,StandardOpenOption.CREATE,StandardOpenOption.WRITE)
            .tryLock();
    }

    /**
     * Release the lock acquired by a previous successful call to LockFile.
     *
     * // REQUIRES: lock was returned by a successful LockFile() call
     * // REQUIRES: lock has not already been unlocked
     *
     * @param lock
     * @throws java.io.IOException
     */
    default void unlockFile(FileLock lock) throws IOException {
        lock.release();
        lock.close();
    }

    /**
     * Arrange to run "(*function)(arg)" once in a background thread.
     *
     * "function" may run in an unspecified thread.
     * Multiple functions added to the same Env may run concurrently in different threads.
     * I.e., the caller may not assume that background work items are serialized.
     *
     * @param task
     * @return
     */
    default Future<?> schedule(Runnable task) {
        return executorService().submit(task);
    }

    static AtomicReference<ExecutorService> executor = new AtomicReference();

    default ExecutorService executorService() {
        ExecutorService e = executor.get();
        if (e == null) {
            executor.compareAndSet(null,
                Executors.newSingleThreadExecutor(threadFactory()));
            e = executor.get();
        }
        return e;
    }

    default ThreadFactory threadFactory() {
        return (r) -> {
            Thread t = new Thread(r);
            t.setName("leveldb-bg");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(
                  (f,e) -> { warn(f.toString()+'('+f.getId()+") "+e.toString()); }
            );
            return t;
        };
    }

//   Start a new thread, invoking "function(arg)" within the new thread.
//   When "function(arg)" returns, the thread will be destroyed.
//   virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

//   // *path is set to a temporary directory that can be used for testing. It may
//   // or many not have just been created. The directory may or may not differ
//   // between runs of the same process, but subsequent calls will return the
//   // same directory.
//   virtual Status GetTestDirectory(std::string* path) = 0;

//   // Create and return a log file for storing informational messages.
//   virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

    /**
     * Returns the number of micro-seconds since some fixed point in time.
     *
     * Only useful for computing deltas of time.
     *
     * @return
     */
    default long nowMicros() {
        return (System.nanoTime() / 1000);
    }

    /**
     * Sleep/delay the thread for the prescribed number of micro-seconds.
     *
     * @param micros
     */
    default void sleepForMicroseconds(int micros) {
        try {
            long millis = micros / 1000;
            int nanos = (micros % 1000) * 1000;
            Thread.sleep(millis,nanos);
        }
        catch (InterruptedException e) {
            warn(e.toString());
        }
    }

    default void warn(String msg) {
        System.getLogger("").log(System.Logger.Level.WARNING,msg);
    }

    /**
     * A utility routine: write "data" to the named file.
     *
     * @param fname
     * @param data
     * @throws IOException
     */
    default void writeStringToFile(Path fname, String data) throws IOException {
        Files.write(fname, data.getBytes(),
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING );
    }

    default void syncFile(OutputStream file) throws IOException {
        if (file instanceof FileOutputStream) {
            ((FileOutputStream)file).getFD().sync();
        }
    }

    /**
     * A utility routine: write "data" to the named file with a file sync.
     *
     * @param fname
     * @param data
     * @throws IOException
     */
    default void writeStringToFileSync(Path fname, String data) throws IOException {
        Files.write(fname, data.getBytes(),
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.SYNC );
    }

    /**
     * A utility routine: read contents of named file into *data.
     *
     * @param fname
     * @return
     * @throws java.io.IOException
     */
    default byte[] readFileToString(Path fname) throws IOException {
        return Files.readAllBytes(fname);
    }

}

/*
    db/env.h

    // A file abstraction for randomly reading the contents of a file.
    class RandomAccessFile {} or 'DirectAccessFile'

    // A file abstraction for reading sequentially through a file.
    class SequentialFile {}

    // A file abstraction for sequential writing.
    // The implementation must provide buffering
    // since callers may append small fragments at a time to the file.
    class WritableFile {}

*/

// // Log the specified data to *info_log if info_log is non-NULL.
// extern void Log(Logger* info_log, const char* format, ...)
// #   if defined(__GNUC__) || defined(__clang__)
//     __attribute__((__format__ (__printf__, 2, 3)))
// #   endif
//     ;

