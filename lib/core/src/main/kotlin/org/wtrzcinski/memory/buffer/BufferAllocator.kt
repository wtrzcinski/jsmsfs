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

import org.wtrzcinski.memory.address.Block
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.BlockSize
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.bitmap.ReleaseResult
import org.wtrzcinski.memory.lock.MemoryFileLock

interface BufferAllocator {

    fun existingBuffer(ref: BlockAddress): MemoryBlockReadWriteMapper

    fun existingChannel(name: String = "", ref: BlockAddress, lock: MemoryFileLock): AbstractMemoryReadWriteBuffer

    fun allocateBuffer(exactBodySize: BlockSize): MemoryBlockReadWriteMapper

    fun allocateBuffer(sizeRange: ClosedRange<DefaultBlockSize>): MemoryBlockReadWriteMapper

    fun allocateBuffer(first: BlockAddress, prev: Block): MemoryBlockReadWriteMapper

    fun allocateChannel(
        lock: MemoryFileLock? = null,
        maxBodySize: DefaultBlockSize? = null,
        exactBodySize: BlockSize? = null,
    ): AbstractMemoryReadWriteBuffer

    fun releaseAll(ref: BlockAddress): ReleaseResult

    fun releaseOne(block: Block): ReleaseResult
}