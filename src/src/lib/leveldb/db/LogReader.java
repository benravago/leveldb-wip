package lib.leveldb.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import java.util.function.Consumer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import lib.leveldb.Slice;
import static lib.leveldb.Status.Code.*;
import static lib.leveldb.db.LogFormat.*;
import static lib.leveldb.io.ByteDecoder.*;

class LogReader implements Iterable<Slice>, Iterator<Slice>, Closeable {

    InputStream in;
    Consumer<Throwable> notify = LogFormat.notify;

    LogReader(InputStream in) {
        this.in = in;
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
        catch (Exception e) { notify.accept(e); }
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
        try {
            return nextSpan();
        }
        catch (Exception e) {
            notify.accept(e);
        }
        return false;
    }

    @Override
    public Slice next() {
        if (hasNext()) {
            var e = new Slice(collect(list,span));
            list.clear();
            return e;
        }
        throw new NoSuchElementException();
    }

    static byte[] collect(List<byte[]> srcs, int length) {
        if (srcs.size() < 2) {
            return srcs.get(0);
        }
        var dest = new byte[length];
        var destPos = 0;
        for (var src : srcs) {
            length = src.length;
            System.arraycopy(src,0,dest,destPos,length);
            destPos += length;
        }
        return dest;
    }

    boolean nextSpan() throws IOException {
        if (type == kFullType || type == kLastType) {
            return readSpan();
        } else {
            throw new IllegalStateException("unexpected type "+type);
        }
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
            var expected_crc = unmask(crc);
            var actual_crc = (int) checksum.getValue();
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
        var len = in.read(b);
        if (len < 0) return false;
        if (len != b.length) throw status(Corruption,"short read");
        remaining -= len;
        offset += len;
        return true;
    }

}