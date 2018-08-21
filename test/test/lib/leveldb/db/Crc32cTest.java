package lib.leveldb.db;

import static lib.leveldb.db.LogFormat.*;

import java.util.zip.CRC32C;
import java.util.zip.Checksum;
import static java.util.Arrays.fill;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class Crc32cTest {

    static Checksum seed(byte[] data, int n) {
        Checksum c = new CRC32C();
        c.update(data,0,n);
        return c;
    }

    static Checksum extend(Checksum c, byte[] data, int n) {
        c.update(data,0,n);
        return c;
    }

    static long value(byte[] data, int n) {
        return seed(data,n).getValue();
    }

    static Checksum seed(String s, int n) { return seed(b(s),n); }
    static Checksum extend(Checksum c, String s, int n) { return extend(c,b(s),n); }
    static long value(String s, int n) { return value(b(s),n); }

    static byte[] b(String s) { return s.getBytes(); }
    static int sizeof(byte[] b) { return b.length; }

    @Test
    public void CRC_StandardResults() {

        // From rfc3720 section B.4.
        byte[] buf = new byte[32];

        fill(buf, (byte)0x00);
        assertEquals(0x8a9136aaL, value(buf, sizeof(buf)));

        fill(buf, (byte)0x0ff);
        assertEquals(0x62a8ab43L, value(buf, sizeof(buf)));

        for (int i = 0; i < 32; i++) {
            buf[i] = (byte)( i );
        }
        assertEquals(0x46dd794eL, value(buf, sizeof(buf)));

        for (int i = 0; i < 32; i++) {
            buf[i] = (byte)( 31 - i );
        }
        assertEquals(0x113fdb5cL, value(buf, sizeof(buf)));

        byte[] data = { 0x01,
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
        assertEquals(0xd9963a56L, value(data, sizeof(data)));
    }

    @Test
    public void CRC_Values() {
        assertNotEquals(value("a", 1), value("foo", 3));
    }

    @Test
    public void CRC_Extend() {
        assertEquals(value("hello world", 11),
                     extend(seed("hello ", 6), "world", 5).getValue());
        }

    @Test
    public void CRC_Mask() {
        int crc = (int) value("foo", 3);
        assertNotEquals(crc, mask(crc));
        assertNotEquals(crc, mask(mask(crc)));
        assertEquals(crc, unmask(mask(crc)));
        assertEquals(crc, unmask(unmask(mask(mask(crc)))));
    }

}
