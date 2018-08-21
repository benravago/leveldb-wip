package lib.util.logging;

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 *  Some friendly methods for {@link java.util.logging.Logger}.
 */
public class Log extends Logger {

    private Log(String name) {
        super(name,null);
    }

    public static Log getLogger(Class<?> c) {
        return getLogger(c.getName());
    }
    public static Log getLogger(String name) {
        var lm = LogManager.getLogManager();
        var l = lm.getLogger(name);
        if (l == null) {
            l = new Log(name);
            lm.addLogger(l);
        }
        return (Log)l;
    }

    public static Level level(String s) {
        switch (s) {
            case "DEBUG": return Level.FINE;
            case "TRACE": return Level.FINER;
            case "WARN":  return Level.WARNING;
            case "ERROR": return Level.SEVERE;
            default: return Level.parse(s);
        }
    }

    public void error(String msg, Object... params) {
        if (isLoggable(Level.SEVERE)) log(Level.SEVERE,null,msg,params);
    }
    public void error(Throwable thrown, String msg, Object... params) {
        if (isLoggable(Level.SEVERE)) log(Level.SEVERE,thrown,msg,params);
    }

    public void warn(String msg, Object... params) {
        if (isLoggable(Level.WARNING)) log(Level.WARNING,null,msg,params);
    }
    public void warn(Throwable thrown, String msg, Object... params) {
        if (isLoggable(Level.WARNING)) log(Level.WARNING,thrown,msg,params);
    }

    public void info(String msg, Object... params) {
        if (isLoggable(Level.INFO)) log(Level.INFO,null,msg,params);
    }

    public void debug(String msg, Object... params) {
        if (isLoggable(Level.FINE)) log(Level.FINE,null,msg,params);
    }

    public void trace(String msg, Object... params) {
        if (isLoggable(Level.FINER)) log(Level.FINER,null,msg,params);
    }

    public void log(Level level, Throwable thrown, String msg, Object... params) {
        var source = source(3);
        var r = new LogRecord(level,msg);
        r.setParameters(params);
        r.setThrown(thrown);
        r.setSourceClassName(source[0]);
        r.setSourceMethodName(source[1]);
        log(r);
    }

    final static StackWalker stack = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

    private static String[] source(int toCaller) {
        return stack.walk( stream -> {
            var f = stream.skip(toCaller).findFirst().get(); // skip Log.source()/log()/info()
            return new String[]{f.getClassName(),f.getMethodName()};
        });
    }

}
