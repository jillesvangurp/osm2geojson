package com.github.jillesvangurp.metrics

import java.util.EnumSet
import java.util.concurrent.atomic.AtomicLong

class EnumCounter<T : Enum<T>>(enumClass: Class<T>) {
    private val counters: Array<AtomicLong>
    private val names: List<String>

    init {
        val all = EnumSet.allOf(enumClass)
        names = all.map { it.name }
        counters = Array(all.size) { AtomicLong() }
    }

    fun inc(value: T): Long = counters[value.ordinal].incrementAndGet()
    fun inc(value: T, delta: Long): Long = counters[value.ordinal].addAndGet(delta)
    fun get(value: T): Long = counters[value.ordinal].get()

    override fun toString(): String = buildString {
        counters.indices.forEach { i ->
            append(names[i]).append(": ").append(counters[i].get())
        }
    }

    companion object {
        @JvmStatic
        fun <F : Enum<F>> counter(enumClass: Class<F>) = EnumCounter(enumClass)
    }
}
