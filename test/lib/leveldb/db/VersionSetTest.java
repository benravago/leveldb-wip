package bsd.leveldb.db;

import java.util.List;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.*;

import bsd.leveldb.Slice;
import static bsd.leveldb.db.DbFormat.*;
import static bsd.leveldb.db.TestUtil.*;

public class VersionSetTest {

    List<FileMetaData> files = new ArrayList<>();
    boolean disjointSortedFiles = true;

    InternalKeyComparator cmp = new InternalKeyComparator(new BytewiseComparator());

    void add(String smallest, String largest) {
        add(smallest,largest,100,100);
    }
    void add(String smallest, String largest, long smallestSeq, long largestSeq ) {
        FileMetaData f = new FileMetaData();
        f.number = files.size() + 1;
        f.smallest = internalKey(s(smallest), smallestSeq, kTypeValue);
        f.largest = internalKey(s(largest), largestSeq, kTypeValue);
        files.add(f);
    }

    int find(String key) {
        InternalKey target = internalKey(s(key), 100, kTypeValue);
        return Version.findFile(cmp, files, target);
    }

    boolean overlaps(String smallest, String largest) {
        Slice s = smallest != null ? s(smallest) : null;
        Slice l = largest != null ? s(largest) : null;
        return Version.someFileOverlapsRange(cmp, disjointSortedFiles, files, s, l);
    }

    @Test
    public void FindFileTest_Empty() {
        assertEquals(0, find("foo"));
        assertTrue(! overlaps("a", "z"));
        assertTrue(! overlaps(null, "z"));
        assertTrue(! overlaps("a", null));
        assertTrue(! overlaps(null, null));
    }

    @Test
    public void FindFileTest_Single() {
        add("p", "q");
        assertEquals(0, find("a"));
        assertEquals(0, find("p"));
        assertEquals(0, find("p1"));
        assertEquals(0, find("q"));
        assertEquals(1, find("q1"));
        assertEquals(1, find("z"));

        assertTrue(! overlaps("a", "b"));
        assertTrue(! overlaps("z1", "z2"));
        assertTrue(overlaps("a", "p"));
        assertTrue(overlaps("a", "q"));
        assertTrue(overlaps("a", "z"));
        assertTrue(overlaps("p", "p1"));
        assertTrue(overlaps("p", "q"));
        assertTrue(overlaps("p", "z"));
        assertTrue(overlaps("p1", "p2"));
        assertTrue(overlaps("p1", "z"));
        assertTrue(overlaps("q", "q"));
        assertTrue(overlaps("q", "q1"));

        assertTrue(! overlaps(null, "j"));
        assertTrue(! overlaps("r", null));
        assertTrue(overlaps(null, "p"));
        assertTrue(overlaps(null, "p1"));
        assertTrue(overlaps("q", null));
        assertTrue(overlaps(null, null));
    }

    @Test
    public void FindFileTest_Multiple() {
        add("150", "200");
        add("200", "250");
        add("300", "350");
        add("400", "450");
        assertEquals(0, find("100"));
        assertEquals(0, find("150"));
        assertEquals(0, find("151"));
        assertEquals(0, find("199"));
        assertEquals(0, find("200"));
        assertEquals(1, find("201"));
        assertEquals(1, find("249"));
        assertEquals(1, find("250"));
        assertEquals(2, find("251"));
        assertEquals(2, find("299"));
        assertEquals(2, find("300"));
        assertEquals(2, find("349"));
        assertEquals(2, find("350"));
        assertEquals(3, find("351"));
        assertEquals(3, find("400"));
        assertEquals(3, find("450"));
        assertEquals(4, find("451"));

        assertTrue(! overlaps("100", "149"));
        assertTrue(! overlaps("251", "299"));
        assertTrue(! overlaps("451", "500"));
        assertTrue(! overlaps("351", "399"));

        assertTrue(overlaps("100", "150"));
        assertTrue(overlaps("100", "200"));
        assertTrue(overlaps("100", "300"));
        assertTrue(overlaps("100", "400"));
        assertTrue(overlaps("100", "500"));
        assertTrue(overlaps("375", "400"));
        assertTrue(overlaps("450", "450"));
        assertTrue(overlaps("450", "500"));
    }

    @Test
    public void FindFileTest_MultiplenullBoundaries() {
        add("150", "200");
        add("200", "250");
        add("300", "350");
        add("400", "450");
        assertTrue(! overlaps(null, "149"));
        assertTrue(! overlaps("451", null));
        assertTrue(overlaps(null, null));
        assertTrue(overlaps(null, "150"));
        assertTrue(overlaps(null, "199"));
        assertTrue(overlaps(null, "200"));
        assertTrue(overlaps(null, "201"));
        assertTrue(overlaps(null, "400"));
        assertTrue(overlaps(null, "800"));
        assertTrue(overlaps("100", null));
        assertTrue(overlaps("200", null));
        assertTrue(overlaps("449", null));
        assertTrue(overlaps("450", null));
    }

    @Test
    public void FindFileTest_OverlapSequenceChecks() {
        add("200", "200", 5000, 3000);
        assertTrue(! overlaps("199", "199"));
        assertTrue(! overlaps("201", "300"));
        assertTrue(overlaps("200", "200"));
        assertTrue(overlaps("190", "200"));
        assertTrue(overlaps("200", "210"));
    }

    @Test
    public void FindFileTest_OverlappingFiles() {
        add("150", "600");
        add("400", "500");
        disjointSortedFiles = false;
        assertTrue(! overlaps("100", "149"));
        assertTrue(! overlaps("601", "700"));
        assertTrue(overlaps("100", "150"));
        assertTrue(overlaps("100", "200"));
        assertTrue(overlaps("100", "300"));
        assertTrue(overlaps("100", "400"));
        assertTrue(overlaps("100", "500"));
        assertTrue(overlaps("375", "400"));
        assertTrue(overlaps("450", "450"));
        assertTrue(overlaps("450", "500"));
        assertTrue(overlaps("450", "700"));
        assertTrue(overlaps("600", "700"));
    }

}