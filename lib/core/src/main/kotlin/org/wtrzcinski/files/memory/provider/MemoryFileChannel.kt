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

package org.wtrzcinski.files.memory.provider

import org.wtrzcinski.files.memory.buffer.channel.FragmentedReadWriteBuffer
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.nio.ByteBuffer
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

internal class MemoryFileChannel(private val delegate: FragmentedReadWriteBuffer) : FileChannel() {

    //    seekable byte channel
    override fun read(dst: ByteBuffer): Int {
        return delegate.read(dst)
    }

    override fun write(src: ByteBuffer): Int {
        return delegate.write(src)
    }

    override fun position(): Long {
        return delegate.position()
    }

    override fun position(newPosition: Long): FileChannel {
        return MemoryFileChannel(delegate.position(newPosition))
    }

    override fun size(): Long {
        return delegate.size()
    }

    override fun truncate(size: Long): FileChannel {
        return MemoryFileChannel(delegate.truncate(size))
    }

    //    file channel
    override fun implCloseChannel() {
        delegate.close()
    }

    override fun map(mode: MapMode, position: Long, size: Long): MappedByteBuffer {
        TODO("Not yet implemented")
    }

    override fun map(mode: MapMode, offset: Long, size: Long, arena: Arena): MemorySegment {
        TODO("Not yet implemented")
    }

    override fun force(metaData: Boolean) {
        TODO("Not yet implemented")
    }

    override fun lock(position: Long, size: Long, shared: Boolean): FileLock {
        TODO("Not yet implemented")
    }

    override fun tryLock(position: Long, size: Long, shared: Boolean): FileLock {
        TODO("Not yet implemented")
    }

    override fun transferTo(position: Long, count: Long, target: WritableByteChannel?): Long {
        TODO("Not yet implemented")
    }

    override fun transferFrom(src: ReadableByteChannel?, position: Long, count: Long): Long {
        TODO("Not yet implemented")
    }

    override fun read(dsts: Array<out ByteBuffer>, offset: Int, length: Int): Long {
        TODO("Not yet implemented")
    }

    override fun read(dst: ByteBuffer, position: Long): Int {
        TODO("Not yet implemented")
    }

    override fun write(src: ByteBuffer, position: Long): Int {
        TODO("Not yet implemented")
    }

    override fun write(srcs: Array<out ByteBuffer>, offset: Int, length: Int): Long {
        TODO("Not yet implemented")
    }
}