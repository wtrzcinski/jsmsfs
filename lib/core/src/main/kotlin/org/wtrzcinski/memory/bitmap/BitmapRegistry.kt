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

package org.wtrzcinski.memory.bitmap

import org.wtrzcinski.memory.address.Block
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.lock.MemoryLockRegistry
import org.wtrzcinski.memory.mode.Mode
import org.wtrzcinski.memory.mode.OpenMode
import org.wtrzcinski.memory.mode.SafeMode
import org.wtrzcinski.memory.mode.UnsafeMode

interface BitmapRegistry {

    val totalByteSize: DefaultBlockSize

    val reserved: BitmapReservedBlocks

    val free: BitmapFreeBlocks

    fun isReadOnly(): Boolean

    fun isReserved(ref: BlockAddress): Boolean

    fun allocate(range: ClosedRange<DefaultBlockSize>, exactBlockSize: DefaultBlockSize): BitmapEntry

    fun allocate(range: ClosedRange<DefaultBlockSize>, first: BlockAddress? = null, prev: BitmapEntry? = null): BitmapEntry

    fun release(block: Block): ReleaseResult

    companion object {
        operator fun invoke(
            memoryOffset: Long,
            memorySize: DefaultBlockSize,
            readOnly: Boolean,
            locks: MemoryLockRegistry,
            compact: Boolean,
        ): BitmapRegistryGroup {
            return BitmapRegistryGroup(
                ref = memoryOffset,
                totalByteSize = memorySize,
                locks = locks,
                mode = if (readOnly) {
                    Mode(OpenMode.Get, UnsafeMode.RequireExisting, SafeMode.All)
                } else {
                    Mode(OpenMode.Post, UnsafeMode.ClearExisting, SafeMode.All)
                },
                compact = compact,
            )
        }
    }
}