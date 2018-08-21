package lib.leveldb.db;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import static lib.leveldb.db.DbFormat.*;

import static lib.leveldb.db.TestUtil.*;

public class VersionEditTest {

    static void testEncodeDecode(VersionEdit edit) {
        byte[] encoded, encoded2;
        encoded = edit.encodeTo();
        VersionEdit parsed = new VersionEdit();
        parsed.decodeFrom(encoded,0,encoded.length);
        // ASSERT_TRUE(s.ok()) << s.ToString();
        encoded2 = parsed.encodeTo();
        assertArrayEquals(encoded, encoded2);
    }

    @Test
    public void VersionEditTest_EncodeDecode() {
        long kBig = 1L << 50;

        VersionEdit edit = new VersionEdit();
        for (int i = 0; i < 4; i++) {
            testEncodeDecode(edit);
            edit.addFile(3, kBig + 300 + i, kBig + 400 + i,
                         internalKey(s("foo"), kBig + 500 + i, kTypeValue),
                         internalKey(s("zoo"), kBig + 600 + i, kTypeDeletion));
            edit.deleteFile(4, kBig + 700 + i);
            edit.setCompactPointer(i, internalKey(s("x"), kBig + 900 + i, kTypeValue));
        }

        edit.setComparatorName("foo");
        edit.setLogNumber(kBig + 100);
        edit.setNextFile(kBig + 200);
        edit.setLastSequence(kBig + 1000);
        testEncodeDecode(edit);
    }

}
