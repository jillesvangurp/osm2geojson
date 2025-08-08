package com.github.jillesvangurp.metrics

import com.github.jillesvangurp.common.DateUtil
import org.slf4j.Logger
import java.util.Date

/**
 * Simple class to log begin and end of activities and their duration in seconds.
 */
class StopWatch private constructor(
    private val log: Logger,
    private val activity: String
) {
    private val start = System.currentTimeMillis().also {
        log.info("started $activity at ${DateUtil.formatIsoDate(Date(it))}")
    }

    fun stop(): Long {
        val stop = System.currentTimeMillis()
        log.info("stopped $activity at ${DateUtil.formatIsoDate(Date(stop))} duration ${(stop - start) / 1000} seconds")
        return stop
    }

    companion object {
        @JvmStatic
        fun time(log: Logger, activity: String) = StopWatch(log, activity)
    }
}
