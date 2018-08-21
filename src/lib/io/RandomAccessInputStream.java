package lib.io;

import java.io.OutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * A RandomAccessInputStream contains an internal ByteChannel
 * that may be read as an input stream.
 * <p>
 * Not implemented InputStream methods:
 * <pre>
 *   boolean markSupported()
 *   void mark(int readlimit)
 *   void reset()
 * </pre>
 */
public class RandomAccessInputStream extends SeekableInputStream {

    public RandomAccessInputStream(Path path) throws IOException {
        channel = Files.newByteChannel(path,StandardOpenOption.READ);
    }

    final SeekableByteChannel channel;

    @Override
    public int available() throws IOException {
        var n = channel.size() - channel.position();
        assert (n <= Integer.MAX_VALUE);
        return (int) n;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private ByteBuffer one;

    @Override
    public synchronized int read() throws IOException {
        if (one == null) one = ByteBuffer.allocate(1);
        return channel.read(one.limit(1).position(0)) < 0 ? -1 : (one.get(0) & 0x0ff);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b,0,b.length);
    }
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return channel.read(ByteBuffer.wrap(b,off,len));
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return read(b,off,len);
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        var buf = new byte[available()];
        var len = channel.read(ByteBuffer.wrap(buf));
        out.write(buf,0,len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n < 0) return 0;
        var r = available();
        if (r > n) r = (int)n;
        channel.position(channel.position()+r);
        return r;
    }

    @Override
    public void seek(long newPosition) throws IOException {
        channel.position(newPosition);
    }

    @Override
    public long length() throws IOException {
        return channel.size();
    }

}
