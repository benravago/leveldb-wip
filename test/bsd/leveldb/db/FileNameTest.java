package bsd.leveldb.db;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import static org.junit.Assert.*;

import static bsd.leveldb.db.FileName.*;
import static bsd.leveldb.db.FileName.FileType.*;

public class FileNameTest {

    static Path p(String s) { return Paths.get(s); }
    static String substring(Path p, int n) { return p.toString().substring(0,n); }

    ParsedFileName parseFileName(Path fname) {
        return parseFileName(fname.toString());
    }
    ParsedFileName parseFileName(String fname) {
        try { return FileName.parseFileName(Paths.get(fname)); }
        catch (Exception e) { System.err.println(e.toString()+": "+fname); return null; }
    }

    class Case {
        String fname;
        long number;
        FileType type;
    }
    Case c(String f, long n, FileType t) {
        Case c = new Case();
        c.fname=f; c.number=n; c.type=t;
        return c;
    }

    @Test
    public void FileNameTest_Parse() {
        ParsedFileName p;

        // Successful parses
        Case[] cases = {
            c( "100.log",            100,   kLogFile ),
            c( "0.log",              0,     kLogFile ),
            c( "0.sst",              0,     kTableFile ),
            c( "0.ldb",              0,     kTableFile ),
            c( "CURRENT",            0,     kCurrentFile ),
            c( "LOCK",               0,     kDBLockFile ),
            c( "MANIFEST-2",         2,     kDescriptorFile ),
            c( "MANIFEST-7",         7,     kDescriptorFile ),
            c( "LOG",                0,     kInfoLogFile ),
            c( "LOG.old",            0,     kInfoLogFile ),
            // "18446744073709551615.log",
            c( Long.toString(Long.MAX_VALUE)+".log", Long.MAX_VALUE, kLogFile )
        };
        for (int i = 0; i < cases.length; i++) {
            String f = cases[i].fname;
            assertNotNull(f, p = parseFileName(f));
            assertEquals(f, cases[i].type, p.type);
            assertEquals(f, cases[i].number, p.number);
        }

        // Errors
        String[] errors = {
            "",
            "foo",
            "foo-dx-100.log",
            ".log",
            "",
            "manifest",
            "CURREN",
            "CURRENTX",
            "MANIFES",
            "MANIFEST",
            "MANIFEST-",
            "XMANIFEST-3",
            "MANIFEST-3x",
            "LOC",
            "LOCKx",
            "LO",
            "LOGx",
            "18446744073709551616.log",
            "184467440737095516150.log",
            "100",
            "100.",
            "100.lop"
        };
        for (int i = 0; i < errors.length; i++) {
            String f = errors[i];
            assertNull(f, parseFileName(f));
        }
    }

    @Test
    public void FileNameTest_Construction() {
        ParsedFileName p;
        Path fname;

        fname = currentFileName(p("foo"));
        assertEquals("foo/", substring(fname, 4));
        assertNotNull(p = parseFileName(fname));
        assertEquals(0, p.number);
        assertEquals(kCurrentFile, p.type);

        fname = lockFileName(p("foo"));
        assertEquals("foo/", substring(fname, 4));
        assertNotNull(p = parseFileName(fname));
        assertEquals(0, p.number);
        assertEquals(kDBLockFile, p.type);

        fname = logFileName(p("foo"), 192);
        assertEquals("foo/", substring(fname, 4));
        assertNotNull(p = parseFileName(fname));
        assertEquals(192, p.number);
        assertEquals(kLogFile, p.type);

        fname = tableFileName(p("bar"), 200);
        assertEquals("bar/", substring(fname, 4));
        assertNotNull(p = parseFileName(fname));
        assertEquals(200, p.number);
        assertEquals(kTableFile, p.type);

        fname = descriptorFileName(p("bar"), 100);
        assertEquals("bar/", substring(fname, 4));
        assertNotNull(p = parseFileName(fname));
        assertEquals(100, p.number);
        assertEquals(kDescriptorFile, p.type);

        fname = tempFileName(p("tmp"), 999);
        assertEquals("tmp/", substring(fname, 4));
        assertNotNull(p = parseFileName(fname));
        assertEquals(999, p.number);
        assertEquals(kTempFile, p.type);
    }

}
