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

package org.wtrzcinski.memory

import org.wtrzcinski.memory.address.*
import org.wtrzcinski.memory.bitmap.BitmapEntry
import org.wtrzcinski.memory.bitmap.BitmapRegistry
import org.wtrzcinski.memory.bitmap.ReleaseResult
import org.wtrzcinski.memory.buffer.*
import org.wtrzcinski.memory.lock.MemoryFileLock
import org.wtrzcinski.memory.mapper.MemoryHeaderMapper
import org.wtrzcinski.memory.mapper.schema.SchemaRegistry
import org.wtrzcinski.memory.mode.Mode
import org.wtrzcinski.memory.mode.OpenMode
import org.wtrzcinski.memory.mode.UnsafeMode
import org.wtrzcinski.memory.util.Check
import java.lang.foreign.MemorySegment
import java.nio.file.NoSuchFileException
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemorySegmentLedger(
    val name: String = "ledger",
    val memory: MemorySegment,
    val bitmap: BitmapRegistry,
    val schemas: SchemaRegistry,
    maxBlockSize: DefaultBlockSize
) : BufferAllocator {

    val headerBytes: DefaultBlockSize get() = schemas.sizeHandler.byteSize() + schemas.refHandler.byteSize()

    val maxBodySize = maxBlockSize - headerBytes

    val minBodySize = schemas.instantHandler.byteSize()

    init {
        Check.isTrue { maxBodySize >= minBodySize }
    }

    fun continuousBuffer(address: BlockAddress, start: BlockAddress, size: DefaultBlockSize): ContinuousReadWriteBuffer {
        val asSlice: MemorySegment = memory.asSlice(start.start, size.size)
        return ContinuousReadWriteBuffer(
            memorySegment = asSlice,
            byteBuffer = asSlice.asByteBuffer(),
            address = address,
            schemas = schemas,
            onRelease = { this.releaseAll(it.flip()) },
        )
    }

    override fun existingBuffer(ref: BlockAddress): MemoryBlockReadWriteMapper {
        Check.isTrue { ref.isValid() }
        if (!this.bitmap.isReserved(ref)) {
            throw NoSuchFileException(name)
        }

//        header
        val bodySizeBuffer = bodySizeBuffer(ref)
        val nextOffsetBuffer = nextOffsetBuffer(ref)
        val header = MemoryHeaderMapper(
            address = ref,
            bodySizeBuffer = bodySizeBuffer,
            nextOffsetBuffer = nextOffsetBuffer,
        )
//        body
        val body = continuousBuffer(
            address = ref,
            start = ref + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            schemas = schemas,
            address = ref,
            header = header,
            body = body,
        )
    }

    override fun existingChannel(name: String, ref: BlockAddress, lock: MemoryFileLock): AbstractMemoryReadWriteBuffer {
        Check.isTrue { ref.isValid() }
        if (!this.bitmap.isReserved(ref)) {
            throw NoSuchFileException(name)
        }

        val first = existingBuffer(ref)
        if (lock.mode.open.idempotent) {
            if (first.readNextAddress() == null) {
                return first.body.onClose { lock.release() }
            }
        }

        val channel = FragmentedReadWriteBuffer(
            iterator = MemoryBlockIterator(
                memory = this,
                first = first,
                mode = lock.mode,
            ),
            onClose = { lock.release() },
        )

        if (lock.mode.open == OpenMode.Post) {
            if (lock.mode.write == UnsafeMode.AppendToExisting) {
                return channel.append()
            } else if (lock.mode.write == UnsafeMode.ClearExisting) {
                return channel.truncate()
            }
        }

        return channel
    }

    override fun allocateBuffer(exactBodySize: BlockSize): MemoryBlockReadWriteMapper {
        val blockRange = (this.headerBytes + minBodySize)..(this.maxBodySize + this.headerBytes)

        val reserved: DefaultBlock = bitmap.allocate(
            range = blockRange,
            exactBlockSize = this.headerBytes + exactBodySize,
        )

//        header
        val bodySize = DefaultBlockSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            address = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeRef(value = BlockAddress.InvalidAddress),
        )
//        body
        val body: AbstractMemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.address + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            schemas = schemas,
            address = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateBuffer(first: BlockAddress, prev: Block): MemoryBlockReadWriteMapper {
        val blockRange = (this.headerBytes + minBodySize)..(maxBodySize + this.headerBytes)

        val reserved: DefaultBlock = bitmap.allocate(
            range = blockRange,
            first = first,
            prev = BitmapEntry(prev, first = first),
        )

//        header
        val bodySize = DefaultBlockSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            address = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeRef(value = BlockAddress.InvalidAddress),
        )
//        body
        val body: AbstractMemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.address + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            schemas = schemas,
            address = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateBuffer(sizeRange: ClosedRange<DefaultBlockSize>): MemoryBlockReadWriteMapper {
        val blockRange = (sizeRange.start + this.headerBytes)..(sizeRange.endInclusive + this.headerBytes)

        val reserved: DefaultBlock = bitmap.allocate(range = blockRange)

//        header
        val bodySize = DefaultBlockSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            address = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeRef(value = BlockAddress.InvalidAddress),
        )
//        body
        val body: AbstractMemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.address + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            schemas = schemas,
            address = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateChannel(
        lock: MemoryFileLock?,
        maxBodySize: DefaultBlockSize?,
        exactBodySize: BlockSize?,
    ): AbstractMemoryReadWriteBuffer {
        if (exactBodySize != null) {
            val first = allocateBuffer(exactBodySize = exactBodySize)
            return first.body.onClose { lock?.release() }
        }
        val maxBodySize1 = maxBodySize ?: this.maxBodySize
        val first = allocateBuffer(sizeRange = this.headerBytes..(maxBodySize1 + this.headerBytes))
        val iterator = MemoryBlockIterator(
            memory = this,
            first = first,
            mode = Mode.create(),
        )
        return FragmentedReadWriteBuffer(iterator = iterator).onClose { lock?.release() }
    }

    override fun releaseOne(block: Block): ReleaseResult {
        return bitmap.release(block = block)
    }

    override fun releaseAll(ref: BlockAddress): ReleaseResult {
        val mode = Mode.create()
        val lock = MemoryFileLock.unlocked(mode)
        val existingBuffer = existingChannel(name = "", ref = ref, lock = lock)
        existingBuffer.skipRemaining()
        existingBuffer.close()
        return existingBuffer.release()
    }

    private fun bodySizeBuffer(offset: BlockAddress): AbstractMemoryReadWriteBuffer {
        val size = schemas.sizeHandler.byteSize()
        return continuousBuffer(address = offset, start = offset, size = size)
    }

    private fun nextOffsetBuffer(offset: BlockAddress): AbstractMemoryReadWriteBuffer {
        val start = offset + schemas.sizeHandler.byteSize()
        val size = schemas.refHandler.byteSize()
        return continuousBuffer(address = offset, start = start, size = size)
    }

    companion object {
        operator fun invoke(
            memory: MemorySegment,
            schemas: SchemaRegistry,
            bitmap: BitmapRegistry,
            maxBlockSize: DefaultBlockSize,
        ): MemorySegmentLedger {
            return MemorySegmentLedger(
                memory = memory,
                bitmap = bitmap,
                maxBlockSize = maxBlockSize,
                schemas = schemas,
            )
        }

    }
}