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

package org.wtrzcinski.files.memory.bitmap

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.lock.MemoryLockRegistry
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.mode.ReadMode
import org.wtrzcinski.files.memory.mode.WriteMode

interface BitmapRegistry {

    val totalByteSize: ByteSize

    val reserved: BitmapReservedBlocks

    val free: BitmapFreeBlocks

    fun isReadOnly(): Boolean

    fun isReserved(ref: BlockAddress): Boolean

    fun allocate(range: ClosedRange<ByteSize>, prev: BitmapEntry? = null): BitmapEntry

    fun allocate(range: ClosedRange<ByteSize>, exactBlockSize: ByteSize, prev: BitmapEntry? = null): BitmapEntry

    fun release(block: Block)

    companion object {
        operator fun invoke(
            memoryOffset: Long,
            memorySize: ByteSize,
            readOnly: Boolean,
            locks: MemoryLockRegistry,
            compact: Boolean,
        ): BitmapRegistryGroup {
            return BitmapRegistryGroup(
                offset = memoryOffset,
                totalByteSize = memorySize,
                locks = locks,
                mode = if (readOnly) {
                    Mode.read()
                } else {
                    Mode(OpenMode.Post, WriteMode.TruncateExisting, ReadMode.Block)
                },
                compact = compact,
            )
        }
    }
}