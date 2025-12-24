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

package org.wtrzcinski.files.memory.allocator

import org.wtrzcinski.files.memory.util.Preconditions.assertTrue
import java.lang.foreign.Arena
import java.lang.foreign.MemorySegment
import java.net.URI
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode.READ_WRITE
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.StandardOpenOption.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.function.Supplier

internal enum class MemoryScopeType {
    HEAP {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            return HeapMemorySegmentFactory()
        }
    },

    GLOBAL {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            return NativeMemorySegmentFactory(
                arenaSupplier = { Arena.global() },
                closeable = false,
            )
        }
    },

    AUTO {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            return NativeMemorySegmentFactory(
                arenaSupplier = { Arena.ofAuto() },
                closeable = false,
            )
        }
    },

    SHARED {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            return NativeMemorySegmentFactory(
                arenaSupplier = { Arena.ofShared() },
                closeable = true,
            )
        }
    },

    CONFINED {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            return NativeMemorySegmentFactory(
                arenaSupplier = { Arena.ofConfined() },
                closeable = true,
            )
        }
    },

    TMP_FILE {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            return FileChannelMemorySegmentFactory(
                path = Files.createTempFile("memory", ".txt"),
                options = setOf(TRUNCATE_EXISTING, READ, WRITE, DELETE_ON_CLOSE, SPARSE),
            )
        }
    },

    PATH {
        override fun createFactory(env: Map<String, *>): MemorySegmentFactory {
            val pathName = env["path"]?.toString() ?: throw IllegalArgumentException("Missing path parameter")
            val options = env["options"]
                ?.toString()
                ?.split(",")
                ?.map { it.trim() }
                ?.map { StandardOpenOption.valueOf(it) }
                ?: emptyList()
            var file: Path = try {
                val uri = URI.create(pathName)
                Path.of(uri)
            } catch (_: IllegalArgumentException) {
                Path.of(pathName)
            }

            val options1 = mutableSetOf(READ, WRITE)
            options1.addAll(options)
            return FileChannelMemorySegmentFactory(
                path = file,
                options = options1,
            )
        }
    }
    ;

    companion object {
        val DEFAULT: MemoryScopeType = HEAP
    }

    abstract fun createFactory(env: Map<String, *>): MemorySegmentFactory

    class HeapMemorySegmentFactory : MemorySegmentFactory {
        override fun allocate(byteSize: Long, byteAlignmen: Long): MemorySegment {
            return MemorySegment.ofArray(ByteArray(byteSize.toInt()))
        }

        override fun close() {
        }
    }

    class FileChannelMemorySegmentFactory(
        path: Path,
        val options: Set<StandardOpenOption>,
    ) : MemorySegmentFactory {
        private val newByteChannel: FileChannel = FileChannel.open(path, options)
        private val arena: Arena = Arena.ofShared()

        override fun allocate(byteSize: Long, byteAlignmen: Long): MemorySegment {
            val memorySegment: MemorySegment = newByteChannel.map(READ_WRITE, 0, byteSize, arena)
            if (!memorySegment.isLoaded()) {
                memorySegment.load()
            }
            return memorySegment
        }

        override fun close() {
            arena.use {
                newByteChannel.close()
            }
        }
    }

    class NativeMemorySegmentFactory(
        private val arenaSupplier: Supplier<Arena>,
        private val closeable: Boolean,
    ) : MemorySegmentFactory {
        @Volatile
        private var arena: Arena? = null

        private val segments: ConcurrentMap<Long, MemorySegment> = ConcurrentHashMap()

        override fun allocate(byteSize: Long, byteAlignmen: Long): MemorySegment {
            if (arena == null) {
                synchronized(this) {
                    if (arena == null) {
                        arena = arenaSupplier.get()
                    }
                }
            }
            return segments.compute(byteSize) { key, value ->
                if (value == null) {
                    arena!!.allocate(byteSize, byteAlignmen)
                } else {
                    assertTrue { value.maxByteAlignment() == byteAlignmen }
                    value
                }
            } as MemorySegment
        }

        override fun close() {
            if (closeable) {
                if (arena != null) {
                    synchronized(this) {
                        if (arena != null) {
                            arena!!.close()
                            arena = null
                        }
                    }
                }
            }
        }
    }
}