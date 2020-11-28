package com.altynnikov.GCPPipipeline.helpers;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class LogHandler extends Handler {
    Level lastLevel = Level.FINEST;

    public Level checkLevel() {
        return lastLevel;
    }

    @Override
    public void publish(LogRecord record) {
        lastLevel = record.getLevel();
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws SecurityException {

    }
}
