package lib.util.logging;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogManager;

/**
 *  A java.util.logging configuration class; see {@link java.util.logging.LogManager#readConfiguration}.
 * <p>
 *  To use, set <code>-Djava.util.logging.config.class=lib.util.logging.LogConfig</code>.
 * <p>
 *  Additionally, set <code>-Djava.util.logging.level= FINEST|FINER|FINE|CONFIG|INFO|WARNING|SEVERE</code>
 *  to specify the initial Level of the root logger ("").
 */
public class LogConfig {

    public LogConfig() {

        try (var in = getClass().getResourceAsStream("/logging.properties")) {
            LogManager.getLogManager().readConfiguration(in);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        setRootLogger();
    }

    public static void setRootLogger() {
        var name = System.getProperty("java.util.logging.level",null);
        if (name != null) {
            try {
                var logger = setRootLoggerLevel(name);
                logger.log(Level.CONFIG, "set java.util.logging.level={0}", logger.getLevel().toString());
            }
            catch (Exception e) {
                System.err.println("could not set java.util.logging.level=" + name + "; " + e.toString());
            }
        }
    }

    public static Logger setRootLoggerLevel(String name) {
        var level = Level.parse(name);
        var logger = Logger.getLogger("");
        for (var handler : logger.getHandlers()) {
            handler.setLevel(level);
        }
        logger.setLevel(level);
        return logger;
    }
}
