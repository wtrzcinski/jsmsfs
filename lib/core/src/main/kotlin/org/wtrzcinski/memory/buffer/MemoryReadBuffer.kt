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
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.InvalidRef
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.UnsignedIntRange
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.UnsignedShortRange
import org.wtrzcinski.memory.util.Check
import java.nio.ByteBuffer
import java.time.Instant

@Suppress("unused")
sealed interface MemoryReadBuffer {

    fun readRef(): BlockAddress?

    fun readSize(): DefaultBlockSize

    fun readLong(): Long

    fun readInt(): Int

    fun readShort(): Short

    fun readByte(): Byte

    fun read(dst: ByteBuffer, length: DefaultBlockSize): Int

    fun read(dst: ByteArray): Int {
        return read(ByteBuffer.wrap(dst), DefaultBlockSize(ByteBuffer.wrap(dst).remaining()))
    }

    fun readUnsignedShort(): Long? {
        val intValue = readShort()
        if (intValue == InvalidRef.toShort()) {
            return null
        }
        val value = java.lang.Short.toUnsignedLong(intValue)
        Check.isTrue { value in UnsignedShortRange }
        return value
    }


    fun readUnsignedInt(): Long? {
        val intValue = readInt()
        if (intValue == InvalidRef.toInt()) {
            return null
        }
        val value = Integer.toUnsignedLong(intValue)
        Check.isTrue { value in UnsignedIntRange }
        return value
    }

    fun skipRemaining(): Long

    fun skipInt() {
        readInt()
    }
}