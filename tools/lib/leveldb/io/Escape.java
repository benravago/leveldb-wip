package lib.leveldb.io;

public interface Escape {

    static final char[] hex = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

    static CharSequence chars(byte[] b) {
        return chars(b,0,b.length);
    }
    static CharSequence chars(byte[] b, int off, int len) {
        return chars(b,off,len,'`'); // 0x60
    }
    static CharSequence chars(byte[] b, int off, int len, char tic) {
        var s = new StringBuilder();
        var d = true;
        len += off;
        for (var i = off; i < len; i++) {
            var c = (char)(b[i] & 0x0ff);
            if (c < 0x20 || c > 0x7E || c == tic) {
                if (d) s.append(tic);
                s.append(hex[c>>>4])
                 .append(hex[c&0xF])
                 .append(tic);
                d = false;
            } else {
                s.append(c);
                d = true;
            }
        }
        return s;
    }

    static String string(byte[] b) {
        return string(b,0,b.length);
    }
    static String string(byte[] b, int off, int len) {
        var str = new StringBuilder();
        len += off;
        for (var i = off; i < len; i++) {
            var c = (char)(b[i] & 0x0ff);
            if (c >= ' ' && c <= '~') {
                str.append(c);
            } else {
                str.append("\\x")
                   .append(hex[c>>>4])
                   .append(hex[c&0xF]);
            }
        }
        return str.toString();
    }

}
