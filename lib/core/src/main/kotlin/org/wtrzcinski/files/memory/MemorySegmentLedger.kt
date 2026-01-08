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

package org.wtrzcinski.files.memory

import org.wtrzcinski.files.memory.address.Block
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.address.DefaultBlock
import org.wtrzcinski.files.memory.bitmap.BitmapEntry
import org.wtrzcinski.files.memory.bitmap.BitmapRegistry
import org.wtrzcinski.files.memory.buffer.*
import org.wtrzcinski.files.memory.lock.MemoryFileLock
import org.wtrzcinski.files.memory.mapper.MemoryBlockReadWriteMapper
import org.wtrzcinski.files.memory.mapper.MemoryHeaderMapper
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.mode.WriteMode
import org.wtrzcinski.files.memory.schema.ValueSchema
import org.wtrzcinski.files.memory.schema.handler.IntAddressHandler
import org.wtrzcinski.files.memory.schema.handler.IntSizeSchemaHandler
import org.wtrzcinski.files.memory.util.Check
import java.lang.foreign.MemorySegment
import java.nio.file.NoSuchFileException
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
class MemorySegmentLedger(
    val name: String = "ledger",
    val memory: MemorySegment,
    val bitmap: BitmapRegistry,
    maxBlockSize: ByteSize,
    val sizeSchema: ValueSchema<ByteSize>,
    val addressSchema: ValueSchema<BlockAddress?>,
) : BufferAllocator {

    val headerBytes: ByteSize get() = sizeSchema.handler.byteSize + addressSchema.handler.byteSize

    val maxBodySize = maxBlockSize - headerBytes

    init {
        Check.isTrue { maxBlockSize >= headerBytes }
    }

    fun continuousBuffer(address: BlockAddress, start: BlockAddress, size: ByteSize): ContinuousReadWriteBuffer {
        val asSlice: MemorySegment = memory.asSlice(start.start, size.size)
        return ContinuousReadWriteBuffer(
            memorySegment = asSlice,
            byteBuffer = asSlice.asByteBuffer(),
            address = address,
            sizeSchema = sizeSchema,
            addressSchema = addressSchema,
            release = { this.releaseAll(it.flip()) },
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
            offset = ref,
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
            offset = ref,
            header = header,
            body = body,
        )
    }

    override fun existingChannel(name: String, ref: BlockAddress, lock: MemoryFileLock): MemoryReadWriteBuffer {
        Check.isTrue { ref.isValid() }
        if (!this.bitmap.isReserved(ref)) {
            throw NoSuchFileException(name)
        }

        val first = existingBuffer(ref)
        if (lock.mode.open.idempotent) {
            if (first.readNextOffset() == null) {
                return first.body.onClose { lock.release() }
            }
        }

        val channel = FragmentedReadWriteBuffer(
            iterator = MemoryBlockIterator(
                memory = this,
                first = first,
                mode = lock.mode,
            ),
            close = { lock.release() },
        )

        if (lock.mode.open == OpenMode.Post) {
            if (lock.mode.write == WriteMode.AppendToExisting) {
                return channel.append()
            } else if (lock.mode.write == WriteMode.TruncateExisting) {
                return channel.truncate()
            }
        }

        return channel
    }

    override fun allocateBuffer(size: ByteSize): MemoryBlockReadWriteMapper {
        val blockRange = (this.headerBytes)..(this.maxBodySize + this.headerBytes)

        val reserved: DefaultBlock = bitmap.allocate(
            range = blockRange,
            exactBlockSize = size + this.headerBytes,
        )

//        header
        val bodySize = ByteSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            offset = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeOffset(value = BlockAddress.InvalidOffset),
        )
//        body
        val body: MemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.offset + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            offset = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateBuffer(prev: Block): MemoryBlockReadWriteMapper {
        val blockRange = (this.headerBytes)..(maxBodySize + this.headerBytes)

        val reserved: DefaultBlock = bitmap.allocate(
            range = blockRange,
            prev = BitmapEntry(prev),
        )

//        header
        val bodySize = ByteSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            offset = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeOffset(value = BlockAddress.InvalidOffset),
        )
//        body
        val body: MemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.offset + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            offset = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateBuffer(sizeRange: ClosedRange<ByteSize>): MemoryBlockReadWriteMapper {
        val blockRange = (sizeRange.start + this.headerBytes)..(sizeRange.endInclusive + this.headerBytes)

        val reserved: DefaultBlock = bitmap.allocate(range = blockRange)

//        header
        val bodySize = ByteSize(value = reserved.size - headerBytes.size)
        val header = MemoryHeaderMapper(
            offset = reserved,
            bodySizeBuffer = bodySizeBuffer(reserved).writeSize(value = bodySize),
            nextOffsetBuffer = nextOffsetBuffer(reserved).writeOffset(value = BlockAddress.InvalidOffset),
        )
//        body
        val body: MemoryReadWriteBuffer = continuousBuffer(
            address = reserved,
            start = header.offset + header.size,
            size = header.readBodySize,
        )
        return MemoryBlockReadWriteMapper(
            memory = this,
            offset = reserved,
            header = header,
            body = body,
        )
    }

    override fun allocateChannel(
        lock: MemoryFileLock?,
        maxBodySize: ByteSize?,
        exactBodySize: ByteSize?,
    ): MemoryReadWriteBuffer {
        if (exactBodySize != null) {
            val first = allocateBuffer(size = exactBodySize)
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

    override fun releaseOne(block: Block) {
        bitmap.release(block = block)
    }

    override fun releaseAll(ref: BlockAddress) {
        val mode = Mode.create()
        val lock = MemoryFileLock.unlocked(mode)
        val existingBuffer = existingChannel(name = "", ref = ref, lock = lock)
        existingBuffer.skipRemaining()
        existingBuffer.close()
        existingBuffer.release()
    }

    private fun bodySizeBuffer(offset: BlockAddress): MemoryReadWriteBuffer {
        return continuousBuffer(address = offset, start = offset, size = sizeSchema.handler.byteSize)
    }

    private fun nextOffsetBuffer(offset: BlockAddress): MemoryReadWriteBuffer {
        return continuousBuffer(address = offset, start = offset + sizeSchema.handler.byteSize, size = addressSchema.handler.byteSize)
    }

    companion object {
        operator fun invoke(
            memory: MemorySegment,
            bitmap: BitmapRegistry,
            maxBlockSize: ByteSize,
        ): MemorySegmentLedger {
            val memoryByteSize = memory.byteSize()
            return MemorySegmentLedger(
                memory = memory,
                bitmap = bitmap,
                maxBlockSize = maxBlockSize,
                sizeSchema = ValueSchema(IntSizeSchemaHandler(memoryByteSize = memoryByteSize)),
                addressSchema = ValueSchema(IntAddressHandler(memoryByteSize = memoryByteSize)),
            )
        }

    }
}