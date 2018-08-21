package lib.leveldb.db;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import java.nio.channels.FileLock;
import java.nio.channels.FileChannel;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import lib.io.SeekableInputStream;
import lib.io.RandomAccessInputStream;

import lib.leveldb.Env;

class FileEnv implements Env {

    @Override
    public InputStream newSequentialFile(Path fname) throws IOException {
        return Files.newInputStream(fname, StandardOpenOption.READ );
    }

    @Override
    public SeekableInputStream newRandomAccessFile(Path fname) throws IOException {
        return new RandomAccessInputStream(fname);
    }

    @Override
    public OutputStream newWritableFile(Path fname) throws IOException {
        return Files.newOutputStream(fname,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING );
    }

    @Override
    public OutputStream newAppendableFile(Path fname) throws IOException {
        return Files.newOutputStream(fname,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.APPEND );
    }

    @Override
    public boolean fileExists(Path fname) {
        return Files.exists(fname);
    }

    @Override
    public Path[] getChildren(Path dir) {
        if (Files.isDirectory(dir)) {
            try (var list = Files.list(dir)) {
                return list.toArray(n -> new Path[n]);
            }
            catch (IOException e) { throw new UncheckedIOException(e); }
        }
        return new Path[0];
    }

    @Override
    public boolean renameFile(Path src) throws IOException {
        if (!Files.exists(src)) return false;
        var ts = Files.getLastModifiedTime(src).toInstant().toString();
        var tmp = src.getParent().resolve(src.getFileName().toString()+'-'+ts);
        Files.move(src,tmp);
        return true;
    }

    @Override
    public void deleteFile(Path fname) throws IOException {
        if (Files.isRegularFile(fname)) Files.delete(fname);
    }

    @Override
    public void createDir(Path dirname) throws IOException {
        if (!fileExists(dirname)) {
            Files.createDirectories(dirname,fileAttribute("rwxr-xr-x"));
        }
    }

    public static FileAttribute<?> fileAttribute(String permissions) {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permissions));
    }

    @Override
    public void deleteDir(Path dirname) throws IOException {
        if (Files.isDirectory(dirname)) Files.delete(dirname);
    }

    @Override
    public long getFileSize(Path fname) throws IOException {
        return Files.size(fname);
    }

    @Override
    public void renameFile(Path src, Path target) throws IOException {
        if (fileExists(src)) Files.move(src,target,StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public FileLock lockFile(Path fname) throws IOException {
        return FileChannel
            .open(fname,StandardOpenOption.CREATE,StandardOpenOption.WRITE)
            .tryLock();
    }

    @Override
    public void unlockFile(FileLock lock) throws IOException {
        try (lock; var c = lock.channel()) {
            lock.release();
        }
    }

    @Override
    public void syncFile(OutputStream file) throws IOException {
        if (file instanceof FileOutputStream) {
            ((FileOutputStream)file).getChannel().force(true);
            // or ((FileOutputStream)file).getFD().sync();
        }
    }

    @Override
    public void writeToFile(Path fname, byte[] data) throws IOException {
        Files.write(fname, data,
            StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.SYNC );
    }

    @Override
    public byte[] readFromFile(Path fname) throws IOException {
        return Files.readAllBytes(fname);
    }

    @Override
    public Future<?> schedule(Runnable task) {
        return executorService().submit(task);
    }

    volatile static ExecutorService executor;

    public static ExecutorService executorService() {
        var e = executor;
        if (e == null) {
            e = executor = Executors.newSingleThreadExecutor(threadFactory());
        }
        return e;
    }

    public static ThreadFactory threadFactory() {
        return (r) -> {
            var t = new Thread(r);
            t.setName("leveldb-bg");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(
                (f,e) -> { warn(f.toString()+'('+f.getId()+") "+e.toString()); }
            );
            return t;
        };
    }

    @Override
    public long nowMicros() {
        return (System.nanoTime() / 1000);
    }

    @Override
    public void sleepForMicroseconds(long micros) {
       try {
            var millis = micros / 1000;
            var nanos = (micros % 1000) * 1000;
            Thread.sleep(millis,(int)nanos);
        }
        catch (InterruptedException e) {
            warn(e.toString());
        }
    }

    public static void warn(String msg) {
        System.getLogger("").log(System.Logger.Level.WARNING,msg);
    }

}
