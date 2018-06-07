package bsd.leveldb.io;

import java.io.InputStream;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

// -Djava.util.logging.config.class=bsd.leveldb.io.LogConfig
// -Djava.util.logging.level= FINEST|FINER|FINE|CONFIG|INFO|WARNING|SEVERE

// https://docs.oracle.com/javase/7/docs/api/java/util/logging/LogManager.html

public class LogConfig {

    public LogConfig() {

        try (InputStream in = getClass().getResourceAsStream("/logging.properties")) {
            LogManager.getLogManager().readConfiguration(in);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        setRootLogger();
    }

    public static void setRootLogger() {
        String name = System.getProperty("java.util.logging.level",null);
        if (name != null) {
            try {
                Logger logger = setRootLoggerLevel(name);
                logger.log(Level.CONFIG, "set java.util.logging.level={0}", logger.getLevel().toString());
            }
            catch (Exception e) {
                System.err.println("could not set java.util.logging.level=" + name + "; " + e.toString());
            }
        }
    }

    public static Logger setRootLoggerLevel(String name) {
        Level level = Level.parse(name);
        Logger logger = Logger.getLogger("");
        for (Handler handler : logger.getHandlers()) {
            handler.setLevel(level);
        }
        logger.setLevel(level);
        return logger;
    }
}
