/**
 * Copyright 2025 Wojciech Trzciński
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wtrzcinski.files.memory.address

import org.wtrzcinski.files.memory.buffer.MemoryReadWriteBuffer.Companion.InvalidRef
import org.wtrzcinski.files.memory.exception.MemoryIllegalArgumentException

data class ByteSize(
    private val value: Long,
    private val shift: ByteSizeShift = ByteSizeShift.noop,
) {
    companion object {
        val InvalidSize: ByteSize = ByteSize(value = InvalidRef)

        val EmptySize: ByteSize = ByteSize(value = 0, shift = ByteSizeShift.noop)

        fun readSize(any: Any?): ByteSize? {
            if (any == null) {
                return null
            } else if (any is ByteArray) {
                val toString = String(any).trim()
                return readSize(toString)
            } else {
                val toString = any.toString().lowercase()
                return readSize(toString)
            }
        }

        fun readSize(toString: String): ByteSize {
            try {
                return ByteSize(value = toString.toLong(), shift = ByteSizeShift.noop)
            } catch (_: NumberFormatException) {
                if (toString.endsWith("kb")) {
                    val take = toString.take(toString.length - 2)
                    return ByteSize(value = take.toLong(), shift = ByteSizeShift.kb)
                } else if (toString.endsWith("k")) {
                    val take = toString.take(toString.length - 1)
                    return ByteSize(value = take.toLong(), shift = ByteSizeShift.kb)
                } else if (toString.endsWith("mb")) {
                    val take = toString.take(toString.length - 2)
                    return ByteSize(value = take.toLong(), shift = ByteSizeShift.mb)
                } else if (toString.endsWith("m")) {
                    val take = toString.take(toString.length - 1)
                    return ByteSize(value = take.toLong(), shift = ByteSizeShift.mb)
                } else if (toString.endsWith("gb")) {
                    val take = toString.take(toString.length - 2)
                    return ByteSize(value = take.toLong(), shift = ByteSizeShift.gb)
                } else if (toString.endsWith("g")) {
                    val take = toString.take(toString.length - 1)
                    return ByteSize(value = take.toLong(), shift = ByteSizeShift.gb)
                } else {
                    throw MemoryIllegalArgumentException()
                }
            }
        }
    }

    constructor(value: Int) : this(value = value.toLong())

    val size: Long = shift.convert(value)

    fun isValid(): Boolean {
        return size != InvalidRef
    }

    fun isEmpty(): Boolean {
        return size == 0L
    }

    fun toInt(): Int {
        return size.toInt()
    }

    fun toLong(): Long {
        return size
    }

    fun toDouble(): Double {
        return size.toDouble()
    }

    operator fun times(multiplier: Long): ByteSize {
        return ByteSize(this.size * multiplier)
    }

    operator fun times(multiplier: Int): ByteSize {
        return ByteSize(this.size * multiplier)
    }

    operator fun plus(other: ByteSize): ByteSize {
        return ByteSize(this.size + other.size)
    }

    operator fun plus(other: Int): ByteSize {
        return ByteSize(this.size + other)
    }

    operator fun plus(other: Long): ByteSize {
        return ByteSize(this.size + other)
    }

    operator fun minus(other: ByteSize): ByteSize {
        return ByteSize(this.size - other.size)
    }

    operator fun minus(other: Int): ByteSize {
        return ByteSize(this.size - other)
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(size=$size)"
    }

    operator fun compareTo(other: ByteSize): Int {
        return size.compareTo(other.size)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ByteSize) return false

        if (size != other.size) return false

        return true
    }

    override fun hashCode(): Int {
        return size.hashCode()
    }
}