package lib.leveldb.io;

import java.util.Arrays;
import java.io.PrintStream;

public interface Hex {

    static String hex(byte b) { return new String(new byte[]{hex[(b>>4)&0x0f],hex[b&0x0f]}); }
    static byte[] hex = {0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,0x38,0x39,0x61,0x62,0x63,0x64,0x65,0x66};

    static void dump(byte[] b) {
        dump(b,0,b.length);
    }

    static void dump(byte[] b, int off, int len) {
        dump(System.out,b,off,len);
    }

    static void dump(PrintStream out, byte[] b, int off, int len) {
        dump(out,0,b,off,len);
    }

    static void dump(PrintStream out, int bias, byte[] b, int off, int len) {
        var a = new byte[80];
        var z = new byte[80];
        var limit = off + len;
        var p = (bias+off) & 0x0f;
        while (off < limit) {
            Arrays.fill(a,(byte)0x20);
            a[60+p] = '|';
            var i = 8;
            var j = (bias+off) & 0x7ffffff0;
            while (i > 0) {
                a[--i] = hex[j&0x0f]; j >>>= 4;
            }
            while (p < 16 && off < limit) {
                i = p * 3 + (p < 8 ? 0 : 1);
                j = b[off] & 0x0ff;
                a[10+i] = hex[j>>>4];
                a[11+i] = hex[j&0x0f];
                a[61+p] = (byte)(j < 0x20 ? '.' : j > 0x7e ? '.' : j);
                p++; off++;
            }
            j = 61+p;
            a[j] = '|';
            a[j+1] = '\n';
            if (cmp(a,10,z,10,48)) {
                a[0]=' '; a[1]=a[2]=a[3]='.'; a[4]='\n';
                j = 3;
            }
            if (a[1] != '.' || z[1] != '.') {
                out.write(a,0,j+2);
            }
            var y = a; a = z; z = y;
            p = 0;
        }
    }

    static boolean cmp(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
        var srcEnd = srcPos + length;
        while (srcPos < srcEnd) if (src[srcPos++] != dest[destPos++]) return false;
        return true;
    }

}
