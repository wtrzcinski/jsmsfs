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

package org.wtrzcinski.files.memory.block

import org.wtrzcinski.files.memory.exception.MemoryUnsupportedOperationException
import org.wtrzcinski.files.memory.channel.MemoryOpenOptions
import org.wtrzcinski.files.memory.ref.BlockStart
import java.lang.AutoCloseable
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
internal class MemoryBlockIterator(
    start: MemoryBlock,
    val mode: MemoryOpenOptions,
) : Iterator<MemoryBlock?>, AutoCloseable {

    private val closed = AtomicBoolean(false)

    private val segments: CopyOnWriteArrayList<MemoryBlock> = CopyOnWriteArrayList()

    private var currentIndex = AtomicInt(0)

    init {
        segments.add(start)
    }

    fun isOpen(): Boolean {
        return !closed.load()
    }

    fun offset(): BlockStart {
        return segments.first()
    }

    fun current(): MemoryBlock {
        checkAccessible()

        return segments.last()
    }

    fun size(): Long {
        val refs = allSegments()
        return refs.sumOf { it.bodySize }
    }

    private fun allSegments(): List<MemoryBlock> {
        val refs = mutableListOf<MemoryBlock>()
        var current: MemoryBlock? = segments.last()
        while (current != null) {
            refs.add(current)
            current = current.readNext()
        }
        return refs
    }

    fun skipRemaining() {
        while (hasNext()) {
            skip()
        }
        val current = current()
        current.skipRemaining()
    }

    fun skip(): MemoryBlock? {
        checkAccessible()

        if (currentIndex.load() >= segments.size) {
            return null
        }

        val current = segments.last()
        current.use { current ->
            val nextRef = current.readNext()
            if (nextRef != null) {
                nextRef.skipRemaining()
                segments.add(nextRef)
                currentIndex += 1
                return nextRef
            } else {
                currentIndex += 1
                return null
            }
        }
    }

    override fun next(): MemoryBlock? {
        checkAccessible()

        if (currentIndex.load() >= segments.size) {
            return null
        }

        val current = segments.last()
        current.use { current ->
            val nextRef = current.readNext()
            if (nextRef != null) {
                if (this.mode.write) {
                    val newBodySize = current.position
                    current.resize(newBodySize)
                    segments.add(nextRef)
                    currentIndex += 1
                    return nextRef
                } else {
                    segments.add(nextRef)
                    currentIndex += 1
                    return nextRef
                }
            } else {
                if (this.mode.write) {
                    val newBodySize = current.position
                    current.resize(newBodySize)
                    val nextRef = current.reserveNext()
                    segments.add(nextRef)
                    currentIndex += 1
                    return nextRef
                } else {
                    currentIndex += 1
                    return null
                }
            }
        }
    }

    override fun hasNext(): Boolean {
        val current = segments.last()
        val nextRef = current.readNextRef()
        return nextRef != null
    }

    override fun close() {
        if (closed.compareAndSet(expectedValue = false, newValue = true)) {
            for (it in segments) {
                it.close()
            }
        }
    }

    private fun checkAccessible() {
        if (!isOpen()) {
            throw MemoryUnsupportedOperationException()
        }
    }
}