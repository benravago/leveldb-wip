package bsd.leveldb.io;

import java.nio.file.Path;
import java.io.OutputStream;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogRecord;
import java.util.logging.Handler;
import java.util.logging.StreamHandler;

public final class Info {

    private volatile static Logger logger;
    private volatile static Level level = Level.INFO;

    public static void log(String format, Object... args) {
        getLogger().log(level,format,args);
    }

    public static void log(Throwable thrown, String format, Object... args) {
        LogRecord r = new LogRecord(level,format);
        r.setParameters(args);
        r.setThrown(thrown);
        getLogger().log(r);
    }

    public static Logger getLogger() {
        return logger != null ? logger : Logger.getAnonymousLogger();
    }
    public static Level getLevel() {
        return level;
    }

    public static void setLevel(Level lvl) {
        level = lvl;
    }

    public static Logger setLogger(String name) {
        return setLogger(Logger.getLogger(name));
    }
    public static Logger setLogger(Logger log) {
        for (Handler h : log.getHandlers()) {
            h.setFormatter(formatter());
        }
        return (logger = log);
    }
    public static Logger setLogger(Path path, OutputStream out) {
        String n = path.toString();
        Logger log = Logger.getLogger(n);
        log.setLevel(Level.ALL);
        log.setUseParentHandlers(false);
        log.addHandler(handler(out));
        return setLogger(log);
    }

    static Handler handler(OutputStream out) {
        return new StreamHandler(out,
           Logger.getLogger("").getHandlers()[0].getFormatter());
    }

    static java.util.logging.Formatter formatter() {
      return new java.util.logging.Formatter() {

        final DateFormat df = new SimpleDateFormat("yyyy/MM/dd-hh:mm:ss.SSSS ");
        final StringBuilder buf = new StringBuilder();
        final java.util.Formatter fmt = new java.util.Formatter(buf);

        @Override
        public String format(LogRecord record) {
            buf.setLength(0);
            buf.append(df.format(new Date(record.getMillis())));
            fmt.format(record.getMessage(),record.getParameters());
            buf.append('\n');
            return fmt.toString();
        }
      };
    }

    public static void close() {
        if (logger != null) {
            for (Handler h : logger.getHandlers()) {
                if (h instanceof StreamHandler) {
                    StreamHandler s = (StreamHandler)h;
                    s.flush();
                    s.close();
                }
            }
            logger = null;
        }
    }

}
