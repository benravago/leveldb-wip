package lib.leveldb.db;

import java.nio.file.Paths;

import java.io.PrintStream;
import java.io.IOException;
import java.util.Arrays;

import static lib.util.Iteration.*;

import lib.io.SeekableInputStream;
import lib.io.RandomAccessInputStream;

import lib.leveldb.io.Hex;
import lib.leveldb.io.Escape;

import static lib.leveldb.db.DbFormat.*;
import static lib.leveldb.db.TableFile.*;
import static lib.leveldb.io.ByteDecoder.*;

public class SSTable {
    public static void main(String[] args) throws Exception {
        int snap = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        SSTable.print(args[0],snap);
    }

    static boolean ShortSnap = false;

    static void print(String fn, int v) throws Exception {
        ShortSnap = System.getProperties().containsKey("short.snap");
        var err = (v == 1) ? System.out : (v == 2) ? System.err : null;
        var sst = new TableFile(input(fn),comparator());
        sst.verifyChecksums(true);
        var index = new Index(sst).open();
        print(index,System.out,err);
    }

    public static void print(Index index, PrintStream out, PrintStream err) throws IOException {
        footer(index,out,err);
        index.table.open();
        metadata(index,out,err);
        data(index,out,err);
    }

    static SeekableInputStream input(String fn) throws IOException {
        return new RandomAccessInputStream(Paths.get(fn));
    }
    static InternalKeyComparator comparator() {
        return new InternalKeyComparator(new BytewiseComparator());
    }

//  static byte[] read(Index ix, int offset, int length) throws IOException {
//      var buf = new byte[length];
//      ix.table.file.seek(offset);
//      ix.table.file.read(buf);
//      return buf;
//  }

    static void snap(PrintStream m, Index ix, int offset, int length) throws IOException {
        if (m == null) return;
        var buf = ix.table.readFully(offset,length);
        snap(m,offset,buf,0,buf.length);
    }

    static void snap(PrintStream m, int bias, byte[] b, int off, int len) {
        if (m == null) return;
        if (len > 48 && ShortSnap) {
            var p = bias + off;
            var q = ((p + 31) >>> 4) << 4;
            var r = p + len;
            var s = ((r - 15) >>> 4) << 4;
            if (q < s) {
                Hex.dump(m,bias,b,off,q-p);
                m.println(" ... ...");
                Hex.dump(m,bias,b,s-bias,r-s);
                return;
            }
        }
        Hex.dump(m,bias,b,off,len);
    }

    static void footer(Index ix, PrintStream out, PrintStream err) throws IOException {
        var length = kFooterEncodedLength;
        var offset = (int) ix.table.file.length() - length;
        out.format("\nfooter @0%x\n",offset);
        snap(err, ix, offset,length );
    }

    static void metadata(Index ix, PrintStream out, PrintStream err) throws IOException {
        out.format("\nmetaindex @0%x\n",ix.metaindexOffset);
        // snap(err, ix, ix.metaindexOffset,ix.metaindexSize+kBlockTrailerSize );

        var b = ix.table.readContents(ix.metaindexOffset,ix.metaindexSize);
        snap(err,0,b.data,0,b.length);

        var n = 0;
        for (var span : Index.spans(b)) {
            n++;
            var key = new String(b.data,span.keyOffset,span.keyLength);
            out.format("\n%d. key: %s %d@0%x, data: %d@0%x\n",
                n, key, span.keyLength, span.keyOffset, span.dataSize,  span.dataOffset );
            metadata(ix,out,err,key,span);
        }
    }

    static void metadata(Index ix, PrintStream out, PrintStream err, String key, Index.Span span) throws IOException {
        if (key.equals(BloomFilter)) filter(ix,out,err,span.dataOffset,span.dataSize );
        // snap(err, ix, span.dataOffset,span.dataSize );
    }

    static void data(Index ix, PrintStream out, PrintStream err) throws IOException {
        out.format("\nindex @0%x\n",ix.indexOffset);
        // snap(err, ix, ix.indexOffset,ix.indexSize+kBlockTrailerSize );

        var b = ix.table.readContents(ix.indexOffset,ix.indexSize);
        snap(err,0,b.data,0,b.length);

        var n = Block.restartCount(b);
        var m = Block.restartOffset(b,n);
        var r = restarts(b.data, n, m);
        out.format("restarts  %d@0%x %s\n",r.length,m,Arrays.toString(r));

        n = m = 0;
        for (var span : Index.spans(b)) {
            n++;
            var key = key(b.data,span.keyOffset,span.keyLength);
            out.format("\n%d. key: %s %d@0%x, data: %d@0%x\n",
                n, key, span.keyLength, span.keyOffset, span.dataSize, span.dataOffset );
            var block = ix.table.blockReader(span.dataOffset,span.dataSize,false);
            var contents = block.contents;
            for (var e : each(block.elements())) {
                m++;
                var deltaKeySize = e.unsharedBytes - sizeof_SequenceAndType;
                var cs = Escape.chars(contents.data,e.deltaOffset,deltaKeySize);
                var sn = decodeFixed64(block.contents.data,e.deltaOffset+deltaKeySize);
                out.format("  %d. %d+%d k: \"%s\"%08x v: %d@0%x\n",
                    m, e.sharedBytes, deltaKeySize, cs, sn, e.valueLength, e.deltaOffset );
            }
            snap(out,0,contents.data,contents.offset,contents.length);
            r = restarts(contents.data,block.restarts,block.trailer);
            out.format("restarts  %d@0%x %s\n",r.length,block.trailer,Arrays.toString(r));
        }
    }

    static String key(byte[] b, int off, int len) {
        return Dbf.text(parseInternalKey(b,off,len));
    }

    static String[] restarts(byte[] buf, int count, int offset) {
        var r = new String[count];
        for (int i = 0, j = offset; i < r.length; i++, j += 4) {
            var o = decodeFixed32(buf,j);
            r[i] = Integer.toHexString(o);
        }
        return r;
    }

    static String BloomFilter = "filter.leveldb.BuiltinBloomFilter2";

    static void filter(Index ix, PrintStream out, PrintStream err, int offset, int length) throws IOException {
        var b = ix.table.readFully(offset,length);
        var last = b.length-5;
        var trailer = decodeFixed32(b,last);
        var lgBase = decodeFixed8(b,last+4);
        var n = (last-trailer)/4;
        out.format(" n: %d lgBase: 0%x\n",n,lgBase);
        var p = trailer;
        var next = decodeFixed32(b,p);
        for (var i = 1; i <= n; i++) {
            var data = next; p += 4;
            next = decodeFixed32(b,p);
            var len = next - data;
            out.format(" %d. %d@0%x\n",i,len,(data+offset));
            if (!ShortSnap) snap(err,offset,b,data,len);
        }
        if (!ShortSnap) {
            out.format(" 00.\n");
            snap(err,offset,b,trailer,b.length-trailer);
        }
    }

    static String x(int i) {
        var x = Integer.toHexString(i);
        return (x.length()%2) != 0 ? "0"+x : "00"+x;
    }

}

/*
TODO:
  1. BloomFilter details
    1. key: filter.leveldb.BuiltinBloomFilter2 34@03, data: 12710@0146fc

The filter block is formatted as follows:

[filter 0]
[filter 1]
[filter 2]
...
[filter N-1]

[offset of filter 0]                  : 4 bytes
[offset of filter 1]                  : 4 bytes
[offset of filter 2]                  : 4 bytes
...
[offset of filter N-1]                : 4 bytes

[offset of beginning of offset array] : 4 bytes
lg(base)                              : 1 byte

    filters: n, lgBase: xxx,
    1. ddd@xxx
    ...
*/