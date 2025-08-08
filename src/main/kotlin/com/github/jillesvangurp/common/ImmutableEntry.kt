package com.github.jillesvangurp.common

/**
 * Simple generic implementation of an entry that should serve most common purposes.
 */
class ImmutableEntry<K, V>(override val key: K, override val value: V) : Map.Entry<K, V> {
    override fun toString(): String = "$key;$value"
}
