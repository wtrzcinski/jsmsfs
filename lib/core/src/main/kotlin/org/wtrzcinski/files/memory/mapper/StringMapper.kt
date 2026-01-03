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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.BlockOffset
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.mode.ModeState

class StringMapper(
    private val memory: MemorySegmentLedger,
) : BlockBodyMapper, ModeState() {

    private var name: String? = null

    fun writeName(name: String) {
        this.name = name
    }

    override fun flip(): BlockOffset {
        if (tryFlip()) {
            val localName = name
            if (localName == null) {
                return BlockOffset.InvalidOffset
            } else {
                val bodyAlignment = intByteSize + (localName.length * 4)
                val newByteChannel = memory.allocateChannel(bodyAlignment = bodyAlignment)
                newByteChannel.use {
                    newByteChannel.writeString(localName)
                }
                return newByteChannel.address()
            }
        } else {
            throw MemoryIllegalStateException()
        }
    }
}

