/**
 * Copyright 2026 Wojciech Trzciński
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

package org.wtrzcinski.memory.address

import org.wtrzcinski.memory.exception.MemoryIllegalArgumentException
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.InvalidRef
import java.math.BigInteger

data class DefaultBlockSize(
    private val value: Long,
    private val shift: DefaultBlockSizeShift = DefaultBlockSizeShift.noop,
): Comparable<DefaultBlockSize>, BlockSize {

    companion object {
        val InvalidSize: DefaultBlockSize = DefaultBlockSize(value = InvalidRef)

        val EmptySize: DefaultBlockSize = DefaultBlockSize(value = 0)

        val InfinitySize: DefaultBlockSize = DefaultBlockSize(value = Long.MAX_VALUE)

        fun readSize(any: Any?): DefaultBlockSize? {
            return when (any) {
                null -> null
                is Number -> DefaultBlockSize(value = any.toLong())
                is ByteArray -> readSize(String(any).trim())
                else -> readSize(any.toString().lowercase())
            }
        }

        fun readSize(toString: String): DefaultBlockSize {
            try {
                return DefaultBlockSize(value = toString.toLong(), shift = DefaultBlockSizeShift.noop)
            } catch (_: NumberFormatException) {
                if (toString.endsWith("kb")) {
                    val take = toString.take(toString.length - 2)
                    return DefaultBlockSize(value = take.toLong(), shift = DefaultBlockSizeShift.kb)
                } else if (toString.endsWith("k")) {
                    val take = toString.take(toString.length - 1)
                    return DefaultBlockSize(value = take.toLong(), shift = DefaultBlockSizeShift.kb)
                } else if (toString.endsWith("mb")) {
                    val take = toString.take(toString.length - 2)
                    return DefaultBlockSize(value = take.toLong(), shift = DefaultBlockSizeShift.mb)
                } else if (toString.endsWith("m")) {
                    val take = toString.take(toString.length - 1)
                    return DefaultBlockSize(value = take.toLong(), shift = DefaultBlockSizeShift.mb)
                } else if (toString.endsWith("gb")) {
                    val take = toString.take(toString.length - 2)
                    return DefaultBlockSize(value = take.toLong(), shift = DefaultBlockSizeShift.gb)
                } else if (toString.endsWith("g")) {
                    val take = toString.take(toString.length - 1)
                    return DefaultBlockSize(value = take.toLong(), shift = DefaultBlockSizeShift.gb)
                } else {
                    throw MemoryIllegalArgumentException()
                }
            }
        }
    }

    constructor(value: Int) : this(value = value.toLong())

    override val size: Long = shift.convert(value)

    fun asString(): String {
        return size.toString()
    }

    fun isValid(): Boolean {
        return size != InvalidRef
    }

    fun isEmpty(): Boolean {
        return size == 0L
    }

    fun isInfinity(): Boolean {
        return size == Long.MAX_VALUE
    }

    fun toAddress(): DefaultBlockAddress {
        return DefaultBlockAddress(size)
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

    fun toBigInteger(): BigInteger {
        return BigInteger.valueOf(toLong())
    }

    operator fun plus(other: BlockSize): DefaultBlockSize {
        return DefaultBlockSize(Math.addExact(this.size, other.size))
    }

    operator fun plus(other: Int): DefaultBlockSize {
        return DefaultBlockSize(Math.addExact(this.size, other.toLong()))
    }

    operator fun plus(other: Long): DefaultBlockSize {
        return DefaultBlockSize(Math.addExact(this.size, other))
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(size=$size)"
    }

    override operator fun compareTo(other: DefaultBlockSize): Int {
        return size.compareTo(other.size)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DefaultBlockSize) return false

        if (size != other.size) return false

        return true
    }

    override fun hashCode(): Int {
        return size.hashCode()
    }
}