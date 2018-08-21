package bsd.leveldb.db;

import java.io.IOException;
import java.util.function.Consumer;

import bsd.leveldb.Status;

/**
 * Log format information shared by reader and writer.
 * See ../doc/log_format.md for more detail.
 */
class LogFormat {

    /* enum RecordType */

    // Zero is reserved for preallocated files
    static final int kZeroType = 0;

    static final int kFullType = 1;

    // For fragments
    static final int kFirstType = 2;
    static final int kMiddleType = 3;
    static final int kLastType = 4;


    static final int kMaxRecordType = kLastType;

    static final int kBlockSize = 32768;

    // Header is checksum (4 bytes), length (2 bytes), type (1 byte).
    static final int kHeaderSize = 4 + 2 + 1;

    // default Exception handler for Record{Reader,Writer}'s
    static Consumer<Throwable> notify = (t) -> {
        throw (t instanceof RuntimeException) ? (RuntimeException)t
            : new Status(t).state(t instanceof IOException ? Status.Code.IOError : Status.Code.Fault );
    };

    static Status status(Status.Code code, Throwable t) {
        throw new Status(t).state(code);
    }

    static Status status(Status.Code code, String msg) {
        throw new Status(msg).state(code);
    }

    // CRC32C masking
    //
    // Motivation: it is problematic to compute the CRC of a string that
    // contains embedded CRCs.  Therefore we recommend that CRCs stored
    // somewhere (e.g., in files) should be masked before being stored.

    final static int kMaskDelta = 0x0a282ead8;

    // Return a masked representation of crc.
    static int mask(int crc) {
        // Rotate right by 15 bits and add a constant.
        return  ((crc >>> 15) | (crc << 17)) + kMaskDelta;
    }

    // Return the crc whose masked representation is masked_crc.
    static int unmask(int masked_crc) {
        int rot = masked_crc - kMaskDelta;
        return ((rot >>> 17) | (rot << 15));
    }

}