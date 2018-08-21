package lib.leveldb.io;

import java.io.PrintStream;
import java.io.ByteArrayInputStream;

public class ByteArray extends ByteArrayInputStream {

    public ByteArray(byte[] buf) { super(buf); }
    public ByteArray(byte[] buf, int offset, int length) { super(buf,offset,length); }

    public byte[] array() { return buf; }
    public int count() { return count; }
    public int mark() { return mark; }
    public int position() { return pos; }

    public void snap(PrintStream out) { snap(out,pos); }
    public void snap(PrintStream out, int end) { snap(out,mark,end-mark); }

    public void snap(PrintStream out, int offset, int length) {
        Hex.dump(out,buf,offset,length);
    }
}
