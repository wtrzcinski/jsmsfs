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

package org.wtrzcinski.files.memory.bitmap

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.lock.MemoryLockRegistry
import org.wtrzcinski.files.memory.mode.Mode

interface BitmapRegistry {

    val totalByteSize: ByteSize

    val reserved: BitmapReservedBlocks

    val free: BitmapFreeBlocks

    fun isReadOnly(): Boolean

    fun allocate(name: String, minBlockSize: ByteSize, maxBlockSize: ByteSize, prev: BlockStart): BitmapEntry

    fun release(block: Block)

    companion object {
        operator fun invoke(
            memoryOffset: Long,
            memorySize: ByteSize,
            readOnly: Boolean,
            lockRegistry: MemoryLockRegistry
        ): BitmapRegistryGroup {
            return BitmapRegistryGroup(
                offset = memoryOffset,
                totalByteSize = memorySize,
                locks = lockRegistry,
                mode = Mode.of(readOnly),
            )
        }
    }
}