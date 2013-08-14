package com.github.jillesvangurp.metrics;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EnumCounter<T extends Enum<T>> {
    private final AtomicLong[] counters;
    private final List<String> names;

    public EnumCounter(Class<T> enumClass) {
        EnumSet<T> all = EnumSet.allOf(enumClass);
        names=new ArrayList<>(all.size());
        counters = new AtomicLong[all.size()];
        for(Enum<T> e:all) {
            names.add(e.ordinal(), e.name());
            counters[e.ordinal()] = new AtomicLong();
        }
    }

    public static <F extends Enum<F>> EnumCounter<F> counter(Class<F> enumClass) {
        return new EnumCounter<>(enumClass);
    }

    public long inc(Enum<T> value) {
        return counters[value.ordinal()].incrementAndGet();
    }

    public long inc(Enum<T> value, long delta) {
        return counters[value.ordinal()].addAndGet(delta);
    }

    public long get(Enum<T> value) {
        return counters[value.ordinal()].get();
    }

    @Override
    public String toString() {
        StringBuilder buf= new StringBuilder();
        for(int i=0; i<counters.length; i++) {
            AtomicLong c = counters[i];
            buf.append(names.get(i) + ": " + c.get());
        }

        return buf.toString();
    }
}
