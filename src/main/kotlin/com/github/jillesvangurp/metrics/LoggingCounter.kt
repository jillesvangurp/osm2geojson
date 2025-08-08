package com.github.jillesvangurp.metrics

import org.slf4j.Logger
import java.io.Closeable
import java.util.concurrent.atomic.AtomicLong

class LoggingCounter private constructor(
    private val logger: Logger,
    private val activity: String,
    private val unit: String,
    private val modulo: Int
) : Closeable {
    private val counter = AtomicLong(0)
    private val stopWatch = StopWatch.time(logger, activity)

    fun inc() {
        val newVal = counter.incrementAndGet()
        if (newVal % modulo == 0L) {
            logger.info("$activity: $newVal $unit")
        }
    }

    override fun close() {
        stopWatch.stop()
        logger.info("completed $activity: ${counter.get()} $unit")
    }

    companion object {
        @JvmStatic
        fun counter(log: Logger, name: String, unit: String, modulo: Int) =
            LoggingCounter(log, name, unit, modulo)
    }
}
