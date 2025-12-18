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

package org.wtrzcinski.files.memory.byteBuffer

import org.wtrzcinski.files.memory.ref.BlockStart
import java.lang.foreign.MemorySegment

internal class LongMemoryBlockByteBuffer(
    memorySegment: MemorySegment,
) : MemoryBlockByteBuffer(
    memorySegment = memorySegment,
    byteBuffer = memorySegment.asByteBuffer(),
) {
    override fun readRef(): BlockStart {
        val value = byteBuffer.getLong()
        if (value == invalidRef) {
            return BlockStart.Invalid
        }
        require(value >= 0)
        return BlockStart.of(value)
    }

    override fun writeRef(value: BlockStart) {
        if (!value.isValid()) {
            putLong(invalidRef)
        } else {
            require(value.start >= 0)
            byteBuffer.putLong(value.start)
        }
    }

    override fun writeSize(value: Long) {
        require(value >= 0)
        byteBuffer.putLong(value)
    }
}