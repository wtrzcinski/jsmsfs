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

import org.wtrzcinski.files.memory.byteBuffer.MemoryBlockByteBuffer
import org.wtrzcinski.files.memory.channel.MemoryOpenOptions
import org.wtrzcinski.files.memory.channel.MemorySeekableByteChannel
import org.wtrzcinski.files.memory.ref.Block
import org.wtrzcinski.files.memory.ref.BlockStart
import org.wtrzcinski.files.memory.lock.MemoryFileLock

internal class MemoryBlock(
    private val segments: MemoryBlockRegistry,
    override var start: Long,
    initialBodySize: Long? = null,
    initialNextRef: BlockStart? = null,
) : Block, AutoCloseable {

    init {
        if (initialBodySize != null) {
            val byteBuffer = newBodySizeBuffer()
            byteBuffer.writeSize(value = initialBodySize)
        }
        if (initialNextRef != null) {
            val byteBuffer = newNextRefBuffer()
            byteBuffer.writeRef(value = initialNextRef)
        }
    }

    override val size: Long get() = bodySize + segments.headerSize

    val headerSize: Long get() = segments.headerSize

    val bodySize: Long
        get() {
            val byteBuffer = newBodySizeBuffer()
            return byteBuffer.readRef().start
        }

    val bodyBuffer: MemoryBlockByteBuffer by lazy {
        newBodyBuffer()
    }

    val position: Int
        get() {
            return bodyBuffer.position()
        }

    fun newByteChannel(mode: MemoryOpenOptions, lock: MemoryFileLock? = null): MemorySeekableByteChannel {
        return MemorySeekableByteChannel(start = this, mode = mode, lock = lock)
    }

    fun remaining(): Int {
        return bodyBuffer.remaining()
    }

    fun skipRemaining() {
        bodyBuffer.skipRemaining()
    }

    fun reserveNext(): MemoryBlock {
        val next = segments.reserveSegment(prevOffset = start)
        require(next.start != this.start)
        val byteBuffer = newNextRefBuffer()
        byteBuffer.writeRef(value = next)
        return next
    }

    fun readNext(): MemoryBlock? {
        val offset = this.readNextRef()
        if (offset != null && offset.isValid()) {
            return segments.findSegment(offset = offset)
        }
        return null
    }

    fun readNextRef(): BlockStart? {
        val byteBuffer = newNextRefBuffer()
        val nextRef = byteBuffer.readRef()
        if (nextRef.isValid()) {
            return nextRef
        }
        return null
    }

    fun resize(newBodySize: Int) {
        val prevByteSize = bodySize.toInt()
        if (newBodySize != prevByteSize) {
            val divide = divide(newBodySize + segments.headerSize)
            segments.releaseAll(other = divide.second)

            val byteBuffer = newBodySizeBuffer()
            byteBuffer.writeSize(value = newBodySize.toLong())
        }
    }

    private fun newBodySizeBuffer(): MemoryBlockByteBuffer {
        val result = segments.buffer(
            offset = start,
            size = segments.bodySizeHeaderSize
        )
        result.clear()
        return result
    }

    private fun newNextRefBuffer(): MemoryBlockByteBuffer {
        val result = segments.buffer(
            offset = start + segments.bodySizeHeaderSize,
            size = segments.nextRefHeaderSize,
        )
        result.clear()
        return result
    }

    private fun newBodyBuffer(): MemoryBlockByteBuffer {
        val result = segments.buffer(
            offset = start + segments.bodySizeHeaderSize + segments.nextRefHeaderSize,
            size = bodySize,
        )
        result.clear()
        return result
    }

    fun release() {
        this.segments.releaseAll(this)
        this.close()
    }

    override fun close() {
    }

    override fun toString(): String {
        val next = readNextRef()?.start
        return "${javaClass.simpleName}(start=$start, end=$end, size=$size, headerSize=$headerSize, bodySize=$bodySize, position=$position, next=$next)"
    }
}