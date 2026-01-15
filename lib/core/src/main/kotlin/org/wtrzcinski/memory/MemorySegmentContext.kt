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

import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.allocator.MemoryScopeType
import org.wtrzcinski.memory.bitmap.BitmapRegistry
import org.wtrzcinski.memory.lock.MemoryLockRegistry
import org.wtrzcinski.memory.mapper.MemoryMapperRegistry
import org.wtrzcinski.memory.mapper.schema.SchemaRegistry
import org.wtrzcinski.memory.mode.ModeMonitor
import java.lang.foreign.MemorySegment

class MemorySegmentContext(
    capacity: DefaultBlockSize,
    scope: MemoryScopeType = MemoryScopeType.DEFAULT,
    blockSize: DefaultBlockSize = DefaultMaxBlockSize,
    env: Map<String, Any?> = mapOf(),
    compact: Boolean = true,
) : ModeMonitor() {

    companion object {
        val DefaultMaxBlockSize = DefaultBlockSize(1024 * 4)
    }

    private val memoryFactory = scope.createFactory(env)

    private val memorySegment: MemorySegment = memoryFactory.allocate(capacity.size)

    val locks: MemoryLockRegistry = MemoryLockRegistry()

    val memoryByteSize = memorySegment.byteSize()

    val schemas: SchemaRegistry = SchemaRegistry(memoryByteSize)

    val ledger: MemorySegmentLedger = MemorySegmentLedger(
        memory = memorySegment,
        maxBlockSize = blockSize,
        schemas = schemas,
        bitmap = BitmapRegistry(
            memoryOffset = 0L,
            memorySize = capacity,
            readOnly = memorySegment.isReadOnly,
            locks = locks,
            compact = compact,
        ),
    )

    val mappers: MemoryMapperRegistry = MemoryMapperRegistry(this)

    override fun close() {
        if (tryClose()) {
            memoryFactory.close()
            locks.close()
        }
    }
}