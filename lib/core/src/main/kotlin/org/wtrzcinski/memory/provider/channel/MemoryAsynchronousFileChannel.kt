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

package org.wtrzcinski.memory.provider.channel

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.channels.FileLock
import java.util.concurrent.Future

class MemoryAsynchronousFileChannel : AsynchronousFileChannel() {
    override fun size(): Long {
        TODO("Not yet implemented")
    }

    override fun truncate(size: Long): AsynchronousFileChannel? {
        TODO("Not yet implemented")
    }

    override fun force(metaData: Boolean) {
        TODO("Not yet implemented")
    }

    override fun <A> lock(position: Long, size: Long, shared: Boolean, attachment: A?, handler: CompletionHandler<FileLock?, in A>?) {
        TODO("Not yet implemented")
    }

    override fun lock(position: Long, size: Long, shared: Boolean): Future<FileLock?>? {
        TODO("Not yet implemented")
    }

    override fun tryLock(position: Long, size: Long, shared: Boolean): FileLock? {
        TODO("Not yet implemented")
    }

    override fun <A> read(dst: ByteBuffer?, position: Long, attachment: A?, handler: CompletionHandler<Int?, in A>?) {
        TODO("Not yet implemented")
    }

    override fun read(dst: ByteBuffer?, position: Long): Future<Int?>? {
        TODO("Not yet implemented")
    }

    override fun <A> write(src: ByteBuffer?, position: Long, attachment: A?, handler: CompletionHandler<Int?, in A>?) {
        TODO("Not yet implemented")
    }

    override fun write(src: ByteBuffer?, position: Long): Future<Int?>? {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    override fun isOpen(): Boolean {
        TODO("Not yet implemented")
    }
}