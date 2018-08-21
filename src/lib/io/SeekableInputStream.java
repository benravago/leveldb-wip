package lib.io;

import java.io.InputStream;
import java.io.IOException;

/**
 * An InputStream that maintains a current position and allows the position to be changed.
 */
public abstract class SeekableInputStream extends InputStream {

    /**
     * Sets the file-pointer offset, measured from the beginning of this input stream,
     * at which the next read occurs.
     *
     * @param  pos  the offset position, measured in bytes from the beginning of the input stream,
     *              at which to set the input stream pointer.
     *
     * @throws IOException - if pos is less than 0 or if an I/O error occurs.
    */
    public abstract void seek(long pos) throws IOException;

    /**
     * Returns the length of this input stream.
     *
     * @return the length of this InputStream, measured in bytes.
     * @throws IOException - if an I/O error occurs
     */
    public abstract long length() throws IOException;
}
