package bsd.leveldb.io;

import java.nio.file.Path;
import java.io.IOException;
import java.io.RandomAccessFile;

public class SeekableFileInputStream extends RandomAccessFile implements SeekableInputStream  {

    public SeekableFileInputStream(Path path) throws IOException {
        super(path.toFile(),"r");
    }
}

// int 	available()
// long skip(long n)

// void mark(int readlimit)
// boolean markSupported()
// void reset()

// byte[] readAllBytes()
// int readNBytes(byte[] b, int off, int len)
// long transferTo(OutputStream out)
