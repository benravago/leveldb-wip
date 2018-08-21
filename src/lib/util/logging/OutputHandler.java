package lib.util.logging;

import java.io.OutputStream;

import java.util.logging.Logger;
import java.util.logging.StreamHandler;

/**
 * A {@link java.util.logging.StreamHandler} with additional
 * methods to allow simple attach/detach to {@link java.util.logging.Logger}'s.
 */
public class OutputHandler extends StreamHandler {

    public OutputHandler(OutputStream out) {
        super();
        setOutputStream(out);
    }

    public boolean attachTo(Logger logger) {
        if (in(logger.getHandlers(),this)) {
            return false;
        }
        logger.addHandler(this);
        logger.setUseParentHandlers(false);
        return true;
    }

    public boolean detachFrom(Logger logger) {
        if (!in(logger.getHandlers(),this)) {
            return false;
        }
        logger.setUseParentHandlers(true);
        logger.removeHandler(this);
        return true;
    }

    static <T> boolean in(T[] a, Object b) {
        for (var i:a) if (i.equals(b)) return true;
        return false;
    }
}
