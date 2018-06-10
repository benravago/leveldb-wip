package bsd.leveldb.db;

import java.nio.file.Paths;

import java.io.PrintStream;
import java.io.IOException;

import bsd.leveldb.Slice;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.TableFile.*;

import bsd.leveldb.io.Hex;
import bsd.leveldb.io.Escape;
import bsd.leveldb.io.Iteration;
import bsd.leveldb.io.SeekableFileInputStream;
import bsd.leveldb.io.SnappyDecoder;
import bsd.leveldb.io.SnappyEncoder;

public class SSTable {
    public static void main(String[] args) throws Exception {
        int snap = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        new SSTable().print(args[0],snap);
    }

    PrintStream out;
    TableFile sst;

    void print(String fn, int v) throws Exception {
        out = (v == 1) ? System.out : (v == 2) ? System.err : null;
        sst = new TableFile(input(fn),comparator());
        sst.verifyChecksums(true);
        footer();
        sst.open();
        metadata();
        data();
    }

    void check() throws IOException {
        sst.readFooter((int)sst.file.length());
        byte[] buf = sst.readFully(sst.file,sst.indexOffset,sst.indexSize);
        // System.out.println("buf "+0+' '+buf.length+' '+sst.indexSize);
        Slice dec = SnappyDecoder.decode(buf,0,sst.indexSize);
        // System.out.println("dec "+dec.offset+' '+dec.length);
        Slice enc = SnappyEncoder.encode(dec);
        // System.out.println("enc "+enc.offset+' '+enc.length);
        assert(enc.length == buf.length);
    }

    SeekableFileInputStream input(String fn) throws IOException {
        return new SeekableFileInputStream(Paths.get(fn));
    }
    InternalKeyComparator comparator() {
        return new InternalKeyComparator(new BytewiseComparator());
    }

    void snap(int offset, int length) throws IOException {
        if (out == null) return;
        byte[] buf = new byte[length];
        sst.file.seek(offset);
        sst.file.read(buf);
        snap(out,offset,buf,0,buf.length);
    }

    void snap(PrintStream m, int bias, byte[] b, int off, int len) {
        if (m == null) return;
        if (len > 48) {
            int p = bias + off;
            int q = ((p + 31) >>> 4) << 4;
            int r = p + len;
            int s = ((r - 15) >>> 4) << 4;
            if (q < s) {
                Hex.dump(m,bias,b,off,q-p);
                m.println("........");
                Hex.dump(m,bias,b,s-bias,r-s);
                return;
            }
        }
        Hex.dump(m,bias,b,off,len);
    }

    void footer() throws IOException {
        int length = kFooterEncodedLength;
        int offset = (int) sst.file.length() - length;
        System.out.println("footer @ 0"+Integer.toHexString(offset));
        snap(offset,length);

    }
    void metadata() throws IOException {
        System.out.println("metaindex @ 0"+Integer.toHexString(sst.metaindexOffset));
        snap(sst.metaindexOffset,sst.metaindexSize+kBlockTrailerSize );

        Slice b = sst.readContents(sst.metaindexOffset,sst.metaindexSize);
        int n = 0;
        for (Block.Index i : Block.index(b)) {
            n++;
            String key = new String(b.data,i.keyOffset,i.keyLength);
            System.out.println(" "+n+". key: "+key+" @ "+x(i.keyOffset)+' '+x(i.keyLength)
                                    +", data: "+x(i.dataOffset)+' '+x(i.dataSize));
            snap(i.dataOffset,i.dataSize);
        }

    }
    void data() throws IOException {
        System.out.println("index @ 0"+Integer.toHexString(sst.indexOffset));
        snap(sst.indexOffset,sst.indexSize+kBlockTrailerSize);

        Slice b = sst.readContents(sst.indexOffset,sst.indexSize);
        int n = 0, m = 0;
        for (Block.Index i : Block.index(b)) {
            n++;
            String key = key(b.data,i.keyOffset,i.keyLength);
            System.out.println(" "+n+". key: "+key+" @ "+x(i.keyOffset)+' '+x(i.keyLength)
                                    +", data: "+x(i.dataOffset)+' '+x(i.dataSize));

            Block k = sst.blockReader(i.dataOffset,i.dataSize,false);
            for (Block.Element e : Iteration.of(k.elements())) {
                m++;
                CharSequence cs = Escape.chars(k.contents.data,e.deltaOffset,e.unsharedBytes-8);
                System.out.println("  "+m+". "+e.sharedBytes+' '+(e.unsharedBytes-8)+" \""+cs+"\" "
                                              +x(e.valueLength)+" @ "+x(e.deltaOffset));
            }
            snap(out,0,k.contents.data,k.contents.offset,k.contents.length);
        }
    }

    static String x(int i) { return "0"+Integer.toHexString(i); }

    static String key(byte[] b, int off, int len) {
        return LogFile.text(parseInternalKey(b,off,len));
    }

}