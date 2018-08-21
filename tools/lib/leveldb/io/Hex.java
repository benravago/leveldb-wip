package bsd.leveldb.io;

import java.io.PrintStream;
import java.util.Arrays;

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
        byte[] a = new byte[80];
        int limit = off + len;
        int p = (bias+off) & 0x0f;
        while (off < limit) {
            Arrays.fill(a,(byte)0x20);
            a[60+p] = '|';
            int i = 8;
            int j = (bias+off) & 0x7ffffff0;
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
            out.write(a,0,j+2);
            p = 0;
        }
    }

}
