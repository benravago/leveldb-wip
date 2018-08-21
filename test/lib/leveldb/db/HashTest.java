package bsd.leveldb.db;

import org.junit.Test;
import static org.junit.Assert.*;

public class HashTest {

    static int hash(byte[] data, int n, int seed) {
        return bsd.leveldb.Slice.hash(data, 0, n, seed);
    }

    static int sizeof(byte[] b) { return b.length; }

    @Test
    public void HASH_SignedUnsignedIssue() {

        byte[] data1 = { (byte)0x62 };
        byte[] data2 = { (byte)0xc3, (byte)0x97 };
        byte[] data3 = { (byte)0xe2, (byte)0x99, (byte)0xa5 };
        byte[] data4 = { (byte)0xe1, (byte)0x80, (byte)0xb9, (byte)0x32 };

        byte[] data5 = { 0x01,
            (byte)0xc0, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x14, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18,
            0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x02, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
        };

        assertEquals(hash(new byte[0], 0, 0xbc9f1d34), 0xbc9f1d34);
        assertEquals(hash(data1, sizeof(data1), 0xbc9f1d34), 0xef1345c4);
        assertEquals(hash(data2, sizeof(data2), 0xbc9f1d34), 0x5b663814);
        assertEquals(hash(data3, sizeof(data3), 0xbc9f1d34), 0x323c078f);
        assertEquals(hash(data4, sizeof(data4), 0xbc9f1d34), 0xed21633a);
        assertEquals(hash(data5, sizeof(data5), 0x12345678), 0xf333dabb);
    }

}
