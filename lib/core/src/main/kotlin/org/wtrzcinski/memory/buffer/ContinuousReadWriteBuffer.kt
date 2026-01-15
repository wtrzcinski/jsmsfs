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

package org.wtrzcinski.memory.buffer

import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.bitmap.ReleaseResult
import org.wtrzcinski.memory.mapper.BlockBodyMapper
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler
import org.wtrzcinski.memory.mapper.schema.SchemaRegistry
import org.wtrzcinski.memory.mode.OpenMode
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer

@Suppress("UsePropertyAccessSyntax", "unused")
class ContinuousReadWriteBuffer(
    val memorySegment: MemorySegment,
    val schemas: SchemaRegistry,
    val address: BlockAddress = BlockAddress(memorySegment.address()),
    val byteBuffer: ByteBuffer = memorySegment.asByteBuffer(),
    private val onClose: (AbstractMemoryReadWriteBuffer) -> Unit = {},
    private val onRelease: (AbstractMemoryReadWriteBuffer) -> ReleaseResult = { ReleaseResult() },
) : AbstractMemoryReadWriteBuffer(
    close = {
        if (!memorySegment.isReadOnly()) {
            if (memorySegment.isMapped()) {
                memorySegment.force()
            }
        }
        onClose.invoke(it)
    },
    release = onRelease,
), BlockBodyMapper {

    override fun all(): Collection<BlockAddress> {
        return listOf(address)
    }

    override fun onClose(onClose: (AbstractMemoryReadWriteBuffer) -> Unit): AbstractMemoryReadWriteBuffer {
        return ContinuousReadWriteBuffer(
            memorySegment = memorySegment,
            address = address,
            byteBuffer = byteBuffer,
            schemas = schemas,
            onClose = onClose,
            onRelease = onRelease,
        )
    }

    override fun readRef(): BlockAddress? {
        return schemas.refHandler.readNullable(this)
    }

    override fun writeRef(value: BlockAddress): MemoryReadWriteBuffer {
        return write(schemas.refHandler, value)
    }

    override fun <T : Any> write(handler: SimpleVarHandler<T>, value: T): MemoryReadWriteBuffer {
        handler.write(this, value)
        return this
    }

    override fun readSize(): DefaultBlockSize {
        return schemas.sizeHandler.read(this)
    }

    override fun writeSize(value: DefaultBlockSize): AbstractMemoryReadWriteBuffer {
        schemas.sizeHandler.write(this, value)
        return this
    }

    override fun count(): Int {
        return 1
    }

    override fun address(): BlockAddress {
        monitor.throwIfDeleting()

        return address
    }

    override fun truncate(): AbstractMemoryReadWriteBuffer {
        monitor.throwIfNotWritable()

        byteBuffer.limit(byteBuffer.position())
        return this
    }

    override fun append(): AbstractMemoryReadWriteBuffer {
        monitor.throwIfNotWritable()

        skipRemaining()
        return this
    }

    /**
     * @see java.nio.Buffer.flip
     */
    override fun flip(mode: OpenMode): BlockAddress {
        if (monitor.tryFlip()) {
            check(byteBuffer.position() > 0)

            byteBuffer.flip()
            return address
        } else {
            monitor.throwIllegalStateException()
        }
    }

    override fun position(newPosition: Long): ContinuousReadWriteBuffer {
        byteBuffer.position(newPosition.toInt())
        return this
    }

    override fun size(): Long {
        return byteBuffer.limit().toLong()
    }

    override fun truncate(size: Long): ContinuousReadWriteBuffer {
        byteBuffer.limit(size.toInt())
        return this
    }

    override fun skipRemaining(): Long {
        val remaining = byteBuffer.remaining()
        val limit = byteBuffer.limit()
        val position = byteBuffer.position()
        if (position != limit) {
            byteBuffer.position(limit)
        }
        return remaining.toLong()
    }

    /**
     * @see java.nio.Buffer.limit
     */
    fun limit(limit: DefaultBlockSize) {
        byteBuffer.limit(limit.toInt())
    }

    /**
     * @see java.nio.Buffer.rewind
     */
    fun rewind(): ByteBuffer {
        return byteBuffer.rewind()
    }

    /**
     * @see java.nio.Buffer.clear
     */
    override fun clear() {
        byteBuffer.clear()
    }

    /**
     * @see java.nio.Buffer.position
     */
    override fun position(): Long {
        return byteBuffer.position().toLong()
    }

    /**
     * @see java.nio.Buffer.remaining
     */
    override fun remaining(): DefaultBlockSize {
        return DefaultBlockSize(value = byteBuffer.remaining())
    }

    /**
     * @see ByteBuffer.getLong
     */
    override fun readLong(): Long {
        return byteBuffer.getLong()
    }

    /**
     * @see ByteBuffer.getInt
     */
    override fun readInt(): Int {
        return byteBuffer.getInt()
    }

    override fun readShort(): Short {
        return byteBuffer.getShort()
    }

    override fun readByte(): Byte {
        return byteBuffer.get()
    }

    /**
     * @see ByteBuffer.get
     */
    override fun read(dst: ByteArray): Int {
        byteBuffer.get(dst)
        return dst.size
    }

    /**
     * @see ByteBuffer.put
     */
    override fun read(dst: ByteBuffer): Int {
        val remaining = byteBuffer.remaining()
        dst.put(byteBuffer)
        return if (remaining == 0) {
            -1
        } else {
            remaining
        }
    }

//    todo wojtek
//    override fun readString(charset: Charset): String {
//        val length = readInt()
//        if (length == 0) {
//            return ""
//        }
//
//        if (this.byteBuffer.hasArray()) {
//            val position = this.byteBuffer.position()
//            val bytes = this.byteBuffer.array()
//            val offset = this.byteBuffer.arrayOffset() + position
//            val string = String(bytes = bytes, offset = offset, length = length, charset = charset)
//            this.byteBuffer.position(position + length)
//            return string
//        }
//
//        val dst = ByteBuffer.allocate(length)
//        val read = read(dst, ByteSize(length))
//        Check.isTrue { read == length }
//        return String(dst.array(), charset)
//    }

    /**
     * @see ByteBuffer.get
     */
    fun read(dst: ByteArray, dstOffset: Int, length: DefaultBlockSize) {
        byteBuffer.get(dst, dstOffset, length.toInt())
    }

    /**
     * @see ByteBuffer.put
     */
    override fun read(dst: ByteBuffer, length: DefaultBlockSize): Int {
        val currentPosition = this.byteBuffer.position()
        val otherPosition = dst.position()

        dst.put(otherPosition, this.byteBuffer, currentPosition, length.toInt())

        this.byteBuffer.position(currentPosition + length.toInt())
        dst.position(otherPosition + length.toInt())
        return length.toInt()
    }

    /**
     * @see ByteBuffer.putLong
     */
    override fun writeLong(value: Long) {
        byteBuffer.putLong(value)
    }

    /**
     * @see ByteBuffer.putInt
     */
    override fun writeInt(value: Int) {
        byteBuffer.putInt(value)
    }

    /**
     * @see ByteBuffer.putShort
     */
    override fun writeShort(value: Short) {
        byteBuffer.putShort(value)
    }

    /**
     * @see ByteBuffer.put
     */
    override fun writeByte(value: Byte) {
        byteBuffer.put(value)
    }

    /**
     * @see ByteBuffer.put
     */
    override fun write(value: ByteArray) {
        byteBuffer.put(value)
    }

    /**
     * @see ByteBuffer.put
     */
    override fun write(src: ByteBuffer): Int {
        val remaining = src.remaining()
        byteBuffer.put(src)
        return remaining
    }

    /**
     * @see ByteBuffer.put
     */
    override fun write(src: ByteBuffer, length: DefaultBlockSize): Int {
        val currentPosition = this.byteBuffer.position()
        val otherPosition = src.position()

        this.byteBuffer.put(currentPosition, src, otherPosition, length.toInt())

        this.byteBuffer.position(currentPosition + length.toInt())
        src.position(otherPosition + length.toInt())
        return length.toInt()
    }

    override fun next(): MemoryBlockReadWriteMapper? {
        return null
    }
}