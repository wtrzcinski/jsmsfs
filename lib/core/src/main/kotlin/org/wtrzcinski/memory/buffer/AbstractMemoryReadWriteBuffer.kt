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
import org.wtrzcinski.memory.mode.ModeMonitor

sealed class AbstractMemoryReadWriteBuffer(
    val close: (AbstractMemoryReadWriteBuffer) -> Unit = {},
    val release: (AbstractMemoryReadWriteBuffer) -> ReleaseResult = { ReleaseResult() },
) : MemoryReadWriteBuffer {

    protected val monitor = ModeMonitor()

    //    SeekableByteChannel
    override fun isOpen(): Boolean {
        return monitor.isOpen()
    }

    final override fun close() {
        if (monitor.tryClose()) {
            this.close.invoke(this)
        }
    }

    //    ByteBuffer
    abstract fun address(): BlockAddress

    abstract fun remaining(): DefaultBlockSize

    //    MemoryReadWriteBuffer
    abstract fun append(): AbstractMemoryReadWriteBuffer

    abstract fun truncate(): AbstractMemoryReadWriteBuffer

    abstract fun onClose(onClose: (AbstractMemoryReadWriteBuffer) -> Unit = {}): AbstractMemoryReadWriteBuffer

    abstract fun count(): Int

    abstract fun next(): MemoryBlockReadWriteMapper?

//    todo wojtek ???
    abstract fun all(): Collection<BlockAddress>

    fun release(): ReleaseResult {
        monitor.throwIfNotClosed()

        if (monitor.tryRelease()) {
            return release.invoke(this)
        } else {
            monitor.throwIllegalStateException()
        }
    }

}