package com.github.jillesvangurp.common

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

/**
 * Set of methods useful for working with iso timestamps.
 */
object DateUtil {
    private const val ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
    private const val FILE_DATETIME_FORMAT = "yyyyMMddHHmmss"

    private val isoDateFormatterThreadLocal = ThreadLocal.withInitial {
        SimpleDateFormat(ISO_DATETIME_FORMAT).apply {
            timeZone = TimeZone.getTimeZone("UTC")
        }
    }

    private val fileDateFormatterThreadLocal = ThreadLocal.withInitial {
        SimpleDateFormat(FILE_DATETIME_FORMAT).apply {
            timeZone = TimeZone.getTimeZone("UTC")
        }
    }

    fun parseIsoDate(dateAsString: String): Date = try {
        isoDateFormatterThreadLocal.get().parse(dateAsString)
    } catch (e: ParseException) {
        throw IllegalArgumentException("error parsing date $dateAsString", e)
    }

    fun formatIsoDate(): String = formatIsoDate(Date())

    fun formatIsoDate(date: Date): String = isoDateFormatterThreadLocal.get().format(date)

    fun formatFileDate(): String = formatFileDate(Date())

    fun formatFileDate(date: Date): String = fileDateFormatterThreadLocal.get().format(date)

    fun now(): Date = Date()

    fun oneHourAgo(): Date = Date(System.currentTimeMillis() - 1000*60*60)

    fun yesterDay(): Date = Date(System.currentTimeMillis() - 1000*60*60*24)

    fun lastWeek(): Date = Date(System.currentTimeMillis() - 1000*60*60*24*7)

    fun lastMonth(): Date = Date(System.currentTimeMillis() - 1000*60*60*24*31)
}
