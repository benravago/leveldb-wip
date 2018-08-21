package bsd.leveldb.db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * File names used by DB code
 */
interface FileName {

    static Path makeFileName(Path name, long number, String suffix) {
        return name.resolve(String.format("%06d.%s",number,suffix));
    }

    /**
     * Return the name of the log file with the specified number in the db named by "dbname".
     * The result will be prefixed with "dbname".
     */
    static Path logFileName(Path dbname, long number) {
        assert (number > 0);
        return makeFileName(dbname, number, "log");
    }
    /**
     * Return the name of the sstable with the specified number in the db named by "dbname".
     * The result will be prefixed with "dbname".
     */
    static Path tableFileName(Path dbname, long number) {
        assert (number > 0);
        return makeFileName(dbname, number, "ldb");
    }
    /**
     * Return the legacy file name for an sstable with the specified number in the db named by "dbname".
     * The result will be prefixed with "dbname".
     */
    static Path sstableFileName(Path dbname, long number) {
        assert (number > 0);
        return makeFileName(dbname, number, "sst");
    }
    /**
     * Return the name of the descriptor file for the db named by "dbname" and the specified incarnation number.
     * The result will be prefixed with "dbname".
     */
    static Path descriptorFileName(Path dbname, long number) {
        assert(number > 0);
        String manifest = String.format("MANIFEST-%06d",number);
        return dbname.resolve(manifest);
    }
    /**
     * Return the name of the current file.
     * This file contains the name of the current manifest file.
     * The result will be prefixed with "dbname".
     */
    static Path currentFileName(Path dbname) {
        return dbname.resolve("CURRENT");
    }
    /**
     * Return the name of the lock file for the db named by "dbname".
     * The result will be prefixed with "dbname".
     */
    static Path lockFileName(Path dbname) {
        return dbname.resolve("LOCK");
    }
    /**
     * Return the name of a temporary file owned by the db named "dbname".
     * The result will be prefixed with "dbname".
     */
    static Path tempFileName(Path dbname, long number) {
        assert (number > 0);
        return makeFileName(dbname, number, "dbtmp");
    }
    /**
     * Return the name of the info log file for "dbname".
     */
    static Path infoLogFileName(Path dbname) {
        return dbname.resolve("LOG");
    }
    /**
     * Return the name of the old info log file for "dbname".
     */
    static Path oldInfoLogFileName(Path dbname) {
        return dbname.resolve("LOG.old");
    }

    /**
     * If filename is a leveldb file, store the type of the file in *type.
     * The number encoded in the filename is stored in *number.
     * If the filename was successfully parsed, returns true.  Else return false.
     *
     * Owned filenames have the form:
     * <pre>
     * dbname/CURRENT
     * dbname/LOCK
     * dbname/LOG
     * dbname/LOG.old
     * dbname/MANIFEST-[0-9]+
     * dbname/[0-9]+.(log|sst|ldb|dbtmp)
     * </pre>
     */
    static ParsedFileName parseFileName(Path fname) {
        String rest = fname.getFileName().toString();

        if (rest.equals("CURRENT")) {
            return new ParsedFileName(fname,0,FileType.kCurrentFile);
        }
        if (rest.equals("LOCK")) {
            return new ParsedFileName(fname,0,FileType.kDBLockFile);
        }
        if (rest.equals("LOG") || rest.equals("LOG.old")) {
            return new ParsedFileName(fname,0,FileType.kInfoLogFile);
        }
        Matcher m;
        m = matcher("MANIFEST-([0-9]+)",rest);
        if (m.matches()) {
            return new ParsedFileName(fname, number(m.group(1)), FileType.kDescriptorFile);
        }
        m = matcher("([0-9]+)\\.(log|sst|ldb|dbtmp)",rest);
        if (m.matches()) {
            return new ParsedFileName(fname, number(m.group(1)), fileTypeBySuffix(m.group(2)) );
        }

        return null;
    }

    static class ParsedFileName {
        ParsedFileName(Path f, long n, FileType t) {
            filename = f; number = n; type = t;
        }
        final Path filename;
        final long number;
        final FileType type;
    }

    static FileType fileTypeBySuffix(String s) {
        switch (s) {
            case "log": return FileType.kLogFile;
            case "sst": return FileType.kTableFile;
            case "ldb": return FileType.kTableFile;
            case "dbtmp": return FileType.kTempFile;
            default: return null;
        }
    }
    static Matcher matcher(String pattern, String text) {
        return Pattern.compile(pattern).matcher(text);
    }
    static long number(String s) {
        return (s != null && !s.isEmpty()) ? Long.parseLong(s) : -1;
    }

    /**
     * Make the CURRENT file point to the descriptor file with the specified number.
     */
    static void setCurrentFile(Env env, Path dbname, long descriptorNumber) throws IOException {
        // Remove leading "dbname/" and add newline to manifest file name
        Path manifest = descriptorFileName(dbname,descriptorNumber);
        String contents = manifest.getFileName().toString() + '\n';
        Path tmp = tempFileName(dbname, descriptorNumber);
        try {
            env.writeStringToFileSync(tmp,contents);
            env.renameFile(tmp,currentFileName(dbname));
        }
        catch (IOException ioe) {
            env.deleteFile(tmp);
            throw ioe;
        }
    }

    static enum FileType {
        kLogFile,
        kDBLockFile,
        kTableFile,
        kDescriptorFile,
        kCurrentFile,
        kTempFile,
        kInfoLogFile  // Either the current one, or an old one
    }
}