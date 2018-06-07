package bsd.leveldb.db;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import java.util.function.Consumer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

import bsd.leveldb.Slice;
import static bsd.leveldb.Status.Code.*;
import static bsd.leveldb.db.LogFormat.*;
import static bsd.leveldb.io.ByteEncoder.*;

// Create a writer that will append data to "*dest".

class LogWriter implements Closeable  {

    OutputStream out;
    Consumer<Throwable> notify = LogFormat.notify;
    // TODO: use notify() to handle Status/IOException's

    LogWriter(OutputStream out) {
        this.out = out;
    }
    LogWriter onError(Consumer<Throwable> exit) {
        notify = exit; return this;
    }
    LogWriter verifyChecksum(boolean check) {
        checksum = check ? new CRC32C() : null; return this;
    }

    @Override
    public void close() {
        try {
            out.flush();
            out.close();
            out = null;
        }
        catch (IOException e) { throw status(IOError,e); }
    }

    Checksum checksum = new CRC32C();

    final static byte[] filler = new byte[kHeaderSize];
    int blockOffset = 0;

    void addRecord(Slice slice) {
        try {
            appendData(slice);
        }
        catch (IOException e) { throw status(IOError,e); }
    }

    void appendData(Slice slice) throws IOException {
        byte[] buf = slice.data;
        int ptr = slice.offset;
        int left = slice.length;

        // Fragment the record if necessary and emit it.
        // Note that if slice is empty,
        // we still want to iterate once to emit a single zero-length record

        boolean begin = true;
        do {
            int leftover = kBlockSize - blockOffset;
            assert (leftover >= 0);
            if (leftover < kHeaderSize) {
                // Switch to a new block
                if (leftover > 0) {
                    // Fill the trailer
                    out.write(filler,0,leftover); // dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
                }
                blockOffset = 0;
            }

            // Invariant: we never leave < kHeaderSize bytes in a block.
            assert (kBlockSize - blockOffset - kHeaderSize >= 0);

            int avail = kBlockSize - blockOffset - kHeaderSize;
            int fragmentLength = (left < avail) ? left : avail;

            int type;
            boolean end = (left == fragmentLength);
            if (begin && end) {
                type = kFullType;
            } else if (begin) {
                type = kFirstType;
            } else if (end) {
                type = kLastType;
            } else {
                type = kMiddleType;
            }

            emitPhysicalRecord(type, buf, ptr, fragmentLength);
            ptr += fragmentLength;
            left -= fragmentLength;
            begin = false;
        }
        while (left > 0);
    }

    void emitPhysicalRecord(int t, byte[] buf, int ptr, int n) throws IOException {
        assert (n <= 0xffff);  // Must fit in two bytes
        assert (blockOffset + kHeaderSize + n <= kBlockSize);

        // Compute the crc of the record type and the payload.
        int crc;
        if (checksum != null) {
            checksum.reset();
            checksum.update(t);
            checksum.update(buf,ptr,n);
            crc = (int) checksum.getValue();
        } else {
            crc = 0;
        }

        // Format the header
        byte[] header = new byte[kHeaderSize];
        encodeFixed32(mask(crc),header,0); // Adjust for storage
        encodeFixed16(n,header,4);
        encodeFixed8(t,header,6);

        // Write the header and the payload
        out.write(header);
        out.write(buf,ptr,n);
        out.flush();

        blockOffset += kHeaderSize + n;
    }

}