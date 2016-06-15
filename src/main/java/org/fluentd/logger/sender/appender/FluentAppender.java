package org.fluentd.logger.sender.appender;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.fluentd.logger.FluentLogger;

public class FluentAppender extends AppenderSkeleton {

	private FluentLogger fluentLogger;

	private String tag = "log4j-appender";

	private String host = "localhost";

	private int port = 24224;

	private String label = "label";

	public void setTag(final String tag) {
		this.tag = tag;
	}

	public void setHost(final String host) {
		this.host = host;
	}

	public void setPort(final int port) {
		this.port = port;
	}

	public void setLabel(final String label) {
		this.label = label;
	}

	@Override
	public void activateOptions() {
		try {
			fluentLogger = FluentLogger.getLogger(tag, host, port);
		} catch (final RuntimeException e) {
			e.printStackTrace(); // It's not possible to log, how to do better ? ...
		}
		super.activateOptions();
	}

	@Override
	public void close() {
		fluentLogger.flush();
	}

	@Override
	public boolean requiresLayout() {
		return false;
	}

	@Override
	protected void append(final LoggingEvent event) {
		final Map<String, Object> messages = new HashMap<String, Object>();
		messages.put("level", event.getLevel().toString());
		messages.put("loggerName", event.getLoggerName());
		messages.put("thread", event.getThreadName());
		messages.put("message", event.getMessage().toString());
		messages.put("msec", event.getTimeStamp() % 1000);
		messages.put("throwableInfo", event.getThrowableInformation() != null ? join(event.getThrowableStrRep(), "\n") : "");
        messages.put("threadName", event.getThreadName());
        messages.put("ndc", event.getNDC());
		
        if(event.locationInformationExists()) {
    		final LocationInfo locationInfo = event.getLocationInformation();
    		messages.put("className", locationInfo.getClassName());
            messages.put("fileName", locationInfo.getFileName());
            messages.put("lineNumber", locationInfo.getLineNumber());
            messages.put("methodName", locationInfo.getMethodName());
        }
        
		fluentLogger.log(label, messages, event.getTimeStamp() / 1000);
	}
	
	public static String join(Object[] array, String separator) {
	    if (array == null) {
            return null;
        }
	    int startIndex = 0;
	    int endIndex = array.length;
        if (separator == null) {
            separator = "";
        }

        // endIndex - startIndex > 0:   Len = NofStrings *(len(firstString) + len(separator))
        //           (Assuming that all Strings are roughly equally long)
        int bufSize = (endIndex - startIndex);
        if (bufSize <= 0) {
            return "";
        }

        bufSize *= ((array[startIndex] == null ? 16 : array[startIndex].toString().length())
                        + separator.length());

        final StringBuilder buf = new StringBuilder(bufSize);

        for (int i = startIndex; i < endIndex; i++) {
            if (i > startIndex) {
                buf.append(separator);
            }
            if (array[i] != null) {
                buf.append(array[i]);
            }
        }
        return buf.toString();
    }
	
}
