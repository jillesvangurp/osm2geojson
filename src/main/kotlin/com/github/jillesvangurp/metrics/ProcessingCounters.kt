package com.github.jillesvangurp.metrics

enum class ProcessingCounters {
    success, failure, exception;

    companion object {
        @JvmStatic
        fun counter(): EnumCounter<ProcessingCounters> = EnumCounter(ProcessingCounters::class.java)
    }
}
