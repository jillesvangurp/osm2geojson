package com.github.jillesvangurp.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Set of methods useful for working with iso timestamps.
 */
public class DateUtil {

    private static final String ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String FILE_DATETIME_FORMAT = "yyyyMMddHHmmss";

    private static final ThreadLocal<SimpleDateFormat> isoDateFormatterThreadLocal = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(ISO_DATETIME_FORMAT);
            // set correct time zone
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return simpleDateFormat;
        }
    };

    private static final ThreadLocal<SimpleDateFormat> fileDateFormatterThreadLocal = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(FILE_DATETIME_FORMAT);
            // set correct time zone
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return simpleDateFormat;
        }
    };

    public static Date parseIsoDate(final String dateAsString) {
        try {
            return isoDateFormatterThreadLocal.get().parse(dateAsString);
        } catch (ParseException e) {
            throw new IllegalArgumentException("error parsing date " + dateAsString,e);
        }
    }

    public static String formatIsoDate() {
        return formatIsoDate(new Date());
    }

    public static String formatIsoDate(Date date) {
        return isoDateFormatterThreadLocal.get().format(date);
    }

    public static String formatFileDate() {
        return formatFileDate(new Date());
    }

    public static String formatFileDate(Date date) {
        return fileDateFormatterThreadLocal.get().format(date);
    }

    public static Date now() {
        return new Date();
    }

    public static Date oneHourAgo() {
        return new Date(System.currentTimeMillis() - 1000*60*60);
    }

    public static Date yesterDay() {
        return new Date(System.currentTimeMillis() - 1000*60*60*24);
    }

    public static Date lastWeek() {
        return new Date(System.currentTimeMillis() - 1000*60*60*24*7);
    }

    public static Date lastMonth() {
        return new Date(System.currentTimeMillis() - 1000*60*60*24*31);
    }

}
