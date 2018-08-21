package lib.util.logging;

import java.util.Arrays;
import java.util.Formatter;
import java.util.ResourceBundle;
import java.text.MessageFormat;

import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
/**
 * A logging {@link java.util.logging.Formatter} that uses the <i>util</i> {@link java.util.Formatter}
 * instead of the <i>text</i> {@link java.text.MessageFormat} to format the log message.
 *
 * @see java.util.logging.Formatter#formatMessage(java.util.logging.LogRecord)
 */
public class PrintFormatter extends SimpleFormatter {

    StringBuilder buf = new StringBuilder();
    Formatter printf = new Formatter(buf);

    @Override
    public synchronized String formatMessage(LogRecord record) {
        String format = record.getMessage();
        ResourceBundle catalog = record.getResourceBundle();
        if (catalog != null) {
            try {
                format = catalog.getString(record.getMessage());
            }
            catch (java.util.MissingResourceException ex) {
                // Drop through.  Use record message as format
                format = record.getMessage();
            }
        }
        // Do the formatting.
        Object parameters[] = record.getParameters();
        try {
            if (parameters == null || parameters.length == 0) {
                // No parameters.  Just return format string.
                return format;
            }
            buf.setLength(0);
            return printf.format(format,parameters).toString();
        }
        catch (Exception ex) {
            // Formatting failed: use localized format string.
            return MessageFormat.format("{0} {1} {2}",
                format, Arrays.toString(parameters), ex.toString());
        }
    }

}