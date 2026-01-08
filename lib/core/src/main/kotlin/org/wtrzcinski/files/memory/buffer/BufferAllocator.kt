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

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper

interface BufferAllocator {

    fun existingBuffer(ref: BlockAddress): MemoryBlockReadWriteMapper

    fun existingChannel(name: String, ref: BlockAddress, lock: MemoryFileLock): MemoryReadWriteBuffer

    fun allocateBuffer(prev: Block): MemoryBlockReadWriteMapper

    fun allocateBuffer(sizeRange: ClosedRange<ByteSize>): MemoryBlockReadWriteMapper

    fun allocateBuffer(size: ByteSize): MemoryBlockReadWriteMapper

    fun allocateChannel(
        lock: MemoryFileLock? = null,
        maxBodySize: ByteSize? = null,
        exactBodySize: ByteSize? = null,
    ): MemoryReadWriteBuffer

    fun releaseAll(ref: BlockAddress)

    fun releaseOne(block: Block)
}