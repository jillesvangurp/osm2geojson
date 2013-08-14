package com.github.jillesvangurp.metrics;

import java.util.Date;

import org.slf4j.Logger;

import com.github.jillesvangurp.common.DateUtil;

/**
 * Simple class to log begin and end of activities and their duration in seconds.
 */
public class StopWatch {
    private final String activity;
    private final Logger log;
    private final long start;

    private StopWatch(Logger log, String activity) {
        this.log = log;
        this.activity = activity;
        start = System.currentTimeMillis();
        log.info("started " + activity + " at " + DateUtil.formatIsoDate(new Date(start)));
    }

    public long stop() {
        long stop = System.currentTimeMillis();
        log.info("stopped " + activity + " at " + DateUtil.formatIsoDate(new Date(stop)) + " duration " + ((stop-start)/1000) + " seconds");

        return stop;
    }

    public static StopWatch time(Logger log, String activity) {
        return new StopWatch(log, activity);
    }
}
