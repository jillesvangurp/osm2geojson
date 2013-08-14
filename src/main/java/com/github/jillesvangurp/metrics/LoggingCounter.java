package com.github.jillesvangurp.metrics;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

public class LoggingCounter implements Closeable {

    private final Logger logger;
    private final String activity;
    private final int modulo;
    private final AtomicLong counter;
    private final StopWatch stopWatch;
    private final String unit;

    private LoggingCounter(Logger logger, String activity, String unit, int modulo) {
        this.logger = logger;
        this.activity = activity;
        this.unit = unit;
        this.modulo = modulo;
        counter = new AtomicLong(0);
        stopWatch = StopWatch.time(logger, activity);
    }

    public static LoggingCounter counter(Logger LOG, String name, String unit, int modulo) {
        return new LoggingCounter(LOG, name, unit, modulo);
    }

    public void inc() {
        long newVal = counter.incrementAndGet();
        if(newVal % modulo == 0) {
            logger.info(activity + ": " + newVal + " " + unit);
        }
    }

    @Override
    public void close() {
        stopWatch.stop();
        logger.info("completed " + activity + ": " + counter.get() + " " + unit);
    }

}
