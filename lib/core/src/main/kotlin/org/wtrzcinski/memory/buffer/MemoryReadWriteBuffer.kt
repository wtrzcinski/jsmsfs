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
import org.wtrzcinski.memory.mapper.Mapper
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler
import org.wtrzcinski.memory.mode.OpenMode
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel

interface MemoryReadWriteBuffer : Mapper, MemoryReadBuffer, MemoryWriteBuffer, SeekableByteChannel {

    //    SeekableByteChannel
    abstract override fun position(): Long

    abstract override fun position(newPosition: Long): MemoryReadWriteBuffer

    abstract override fun truncate(size: Long): MemoryReadWriteBuffer

    override fun read(dst: ByteBuffer): Int {
        return read(dst, DefaultBlockSize(dst.remaining()))
    }

    override fun write(src: ByteBuffer): Int {
        return write(src, DefaultBlockSize(src.remaining()))
    }

    //    ByteBuffer
    abstract override fun writeSize(value: DefaultBlockSize): MemoryReadWriteBuffer

    abstract override fun writeRef(value: BlockAddress): MemoryReadWriteBuffer

    fun <T: Any> write(handler: SimpleVarHandler<T>, value: T): MemoryReadWriteBuffer

    fun clear()

    override fun flip(mode: OpenMode): BlockAddress
}