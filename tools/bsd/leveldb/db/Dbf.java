package bsd.leveldb.db;

public class Dbf {
    public static void main(String[] args) throws Exception {
        if (args[0].contains("MANIFEST-")) Manifest.main(args);
        else if (args[0].endsWith(".log")) Journal.main(args);
        else if (args[0].endsWith(".ldb")) SSTable.main(args);
    }
}
