package bsd.leveldb.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import bsd.leveldb.Slice;
import static bsd.leveldb.Status.Code.*;
import static bsd.leveldb.db.LogFormat.*;
import static bsd.leveldb.io.ByteDecoder.*;

class LogReader implements Iterable<Slice>, Iterator<Slice>, Closeable {

    InputStream in;
    Consumer<Throwable> notify;

    LogReader(InputStream in, Consumer<Throwable> notify) {
        this.in = in; this.notify = notify;
    }

    LogReader(InputStream in) {
        this(in, LogFormat.notify);
    }

    LogReader onError(Consumer<Throwable> exit) {
        notify = exit; return this;
    }
    LogReader verifyChecksum(boolean check) {
        checksum = check ? new CRC32C() : null; return this;
    }

    @Override
    public void close() {
        try {
            in.close();
            in = null;
        }
        catch (IOException e) { throw status(IOError,e); }
    }

    @Override
    public Iterator<Slice> iterator() {
        return this;
    }

    List<byte[]> list = new ArrayList<>();
    int span = 0;

    byte[] header = new byte[kHeaderSize];
    int crc = 0;
    int length = 0;
    int type = kFullType;

    int offset = 0;
    int remaining = kBlockSize;

    Checksum checksum = new CRC32C();

    @Override
    public boolean hasNext() {
        if (!list.isEmpty()) {
            return true;
        }
        if (in == null) {
            return false;
        }
        if (type == kFullType || type == kLastType) {
            try { return readSpan(); }
            catch (IOException e) { notify.accept(status(IOError,e)); }
        } else {
            notify.accept(new IllegalStateException("unexpected type "+type));
        }
        return false;
    }

    @Override
    public Slice next() {
        if (hasNext()) {
            Slice e = new Slice(collect(list,span));
            list.clear();
            return e;
        }
        throw new NoSuchElementException();
    }

    static byte[] collect(List<byte[]> srcs, int length) {
        if (srcs.size() < 2) {
            return srcs.get(0);
        }
        byte[] dest = new byte[length];
        int destPos = 0;
        for (byte[] src : srcs) {
            length = src.length;
            System.arraycopy(src,0,dest,destPos,length);
            destPos += length;
        }
        return dest;
    }

    boolean readSpan() throws IOException {
        span = 0;
        while (readBlock()) {
            switch (type) {
                case kFullType: return true;
                case kFirstType: continue;
                case kMiddleType: continue;
                case kLastType: return true;
                default: throw status(Corruption,"unknown record type "+type);
            }
        }
        return false;
    }

    boolean readBlock() throws IOException {
        if (remaining < kHeaderSize) {
            if (remaining > 0) {
                in.skip(remaining);
            }
            remaining = kBlockSize;
        }

        if (!read(header)) return false;

        crc = decodeFixed32(header,0);
        length = decodeFixed16(header,4);
        type = decodeFixed8(header,6);

        byte[] data;
        if (length > 0) {
            data = new byte[length];
            if (!read(data)) return false;
        } else {
            data = new byte[0];
        }
        if (checksum != null) {
            checksum.reset();
            checksum.update(type);
            checksum.update(data,0,data.length);
            int expected_crc = unmask(crc);
            int actual_crc = (int) checksum.getValue();
            if (expected_crc != actual_crc) {
                throw status(Corruption,"checksum mismatch");
            }
        }
        if (data.length > 0) {
            list.add(data);
            span += data.length;
        }
        return true;
    }

    boolean read(byte[] b) throws IOException {
        int len = in.read(b);
        if (len < 0) return false;
        if (len != b.length) throw status(Corruption,"short read");
        remaining -= len;
        offset += len;
        return true;
    }

}