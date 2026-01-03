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

package org.wtrzcinski.files.memory.buffer

import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.mode.ModeState
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

sealed class MemoryReadWriteBuffer(
    val close: (MemoryReadWriteBuffer) -> Unit = {},
    val release: (MemoryReadWriteBuffer) -> Unit = {},
) : MemoryReadBuffer, MemoryWriteBuffer, SeekableByteChannel {

    private val monitor = ModeState()

    //    SeekableByteChannel
    override fun read(dst: ByteBuffer): Int {
        return read(dst, ByteSize(dst.remaining()))
    }

    override fun isOpen(): Boolean {
        return monitor.isOpen()
    }

    override fun close() {
        if (monitor.tryClose()) {
            this.close.invoke(this)
        }
    }

    abstract override fun position(): Long

    abstract override fun position(newPosition: Long): MemoryReadWriteBuffer

    abstract override fun truncate(size: Long): MemoryReadWriteBuffer

    //    ByteBuffer
    abstract fun address(): BlockOffset

    abstract fun flip(): BlockOffset

    abstract fun remaining(): ByteSize

    abstract fun clear()

    abstract override fun writeOffset(value: BlockOffset): MemoryReadWriteBuffer

    abstract override fun writeSize(value: ByteSize): MemoryReadWriteBuffer

    //    MemoryReadWriteBuffer
    abstract val offsetBytes: ByteSize

    abstract val sizeBytes: ByteSize

    abstract fun count(): Int

    abstract fun append(): MemoryReadWriteBuffer

    abstract fun truncate(): MemoryReadWriteBuffer

    abstract fun onClose(close: (MemoryReadWriteBuffer) -> Unit = {}): MemoryReadWriteBuffer

    open fun release() {
        monitor.checkIsClosed()

        if (monitor.tryRelease()) {
            release.invoke(this)
        } else {
            monitor.throwIllegalStateException()
        }
    }

}