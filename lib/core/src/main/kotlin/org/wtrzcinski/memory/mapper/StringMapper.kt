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

package org.wtrzcinski.memory.mapper

import org.wtrzcinski.memory.MemorySegmentLedger
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.memory.mapper.schema.SchemaRegistry
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.intByteSize
import org.wtrzcinski.memory.mode.ModeMonitor
import org.wtrzcinski.memory.mode.OpenMode

class StringMapper(
    private val schemas: SchemaRegistry,
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
                return BlockAddress.InvalidAddress
            } else {
                val maxBodySize = intByteSize + (localName.length * 4)
                val newByteChannel = memory.allocateChannel(maxBodySize = maxBodySize)
                newByteChannel.use {
                    schemas.stringHandler.write(newByteChannel, localName)
                }
                this.address = newByteChannel.address()
                return checkNotNull(this.address)
            }
        } else {
            throw MemoryIllegalStateException()
        }
    }
}

