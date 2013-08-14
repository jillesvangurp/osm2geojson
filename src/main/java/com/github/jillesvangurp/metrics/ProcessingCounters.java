package com.github.jillesvangurp.metrics;

public enum ProcessingCounters {
    success,failure,exception;

    public static EnumCounter<ProcessingCounters> counter() {
        return new EnumCounter<>(ProcessingCounters.class);
    }
}
