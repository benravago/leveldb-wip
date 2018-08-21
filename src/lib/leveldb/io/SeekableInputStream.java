package bsd.leveldb.io;

import java.io.Closeable;
import java.io.IOException;

/**
 * An input stream of bytes that provides a changeable read location.
 */
public interface SeekableInputStream extends Closeable {

    /**
     * Closes this input stream and releases any system resources associated with the stream.
     *
     * @throws IOException - if an I/O error occurs.
     */
    @Override
    void close() throws IOException;

    /**
     * Reads a byte of data from this input stream.
     * The byte is returned as an integer in the range 0 to 255 (0x00-0x0ff).
     * This method blocks if no input is yet available.
     *
     * @return the next byte of data, or -1 if the end of the stream is reached.
     * @throws IOException - if an I/O error occurs. Not thrown if end-of-file has been reached.
     */
    int read() throws IOException;

    /**
     * Reads up to b.length bytes of data from this input stream into an array of bytes.
     * This method blocks until at least one byte of input is available.
     *
     * @param b - the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data because the end of the input stream has been reached.
     * @throws IOException - If the first byte cannot be read for any reason other than the end of data, if the input stream has been closed, or if some other I/O error occurs.
     */
    int read(byte[] b) throws IOException;

    /**
     * Reads up to len bytes of data from this input stream into an array of bytes.
     * This method blocks until at least one byte of input is available.
     *
     * @param b - the buffer into which the data is read.
     * @param off - the start offset in array b at which the data is written.
     * @param len - the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or -1 if there is no more data because the end of the input stream has been reached.
     * @throws IOException - If the first byte cannot be read for any reason other than end of data, or if the input stream has been closed, or if some other I/O error occurs.
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Sets the data-pointer offset, measured from the beginning of this input stream, at which the next read occurs.
     * The offset may be set beyond the end of the data.
     * Setting the offset beyond the end of the data does not change the data length.
     *
     * @param pos - the offset position, measured in bytes from the beginning of the input stream, at which to set the data pointer.
     * @throws IOException
     */
    void seek(long pos) throws IOException;

    /**
     * Returns the length of this input stream.
     *
     * @return the length of this dataset, measured in bytes.
     * @throws IOException
     */
    long length() throws IOException;

}
