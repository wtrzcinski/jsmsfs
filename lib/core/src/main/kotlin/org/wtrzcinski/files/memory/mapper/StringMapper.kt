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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.mode.ModeMonitor
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.intByteSize

class StringMapper(
    private val memory: MemorySegmentLedger,
    private var address: BlockAddress? = null,
) : BlockBodyMapper, ModeMonitor() {

    private var string: String? = null

    fun address(): BlockAddress {
        return checkNotNull(address)
    }

    fun writeString(name: String) {
        this.string = name
    }

    override fun flip(mode: OpenMode): BlockAddress {
        if (tryFlip()) {
            val localName = string
            if (localName == null) {
                return BlockAddress.InvalidOffset
            } else {
                val maxBodySize = intByteSize + (localName.length * 4)
                val newByteChannel = memory.allocateChannel(maxBodySize = maxBodySize)
                newByteChannel.use {
                    newByteChannel.writeString(localName)
                }
                this.address = newByteChannel.address()
                return checkNotNull(this.address)
            }
        } else {
            throw MemoryIllegalStateException()
        }
    }
}

