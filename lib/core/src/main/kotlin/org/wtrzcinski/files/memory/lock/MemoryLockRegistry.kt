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

package org.wtrzcinski.files.memory.lock

import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.buffer.MemoryOpenOptions
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.decrementAndFetch
import kotlin.concurrent.atomics.incrementAndFetch

@OptIn(ExperimentalAtomicApi::class)
class MemoryLockRegistry : AutoCloseable {

    private val locks = ConcurrentHashMap<Long, ReadWriteMemoryFileLock>()

    fun newLock(offset: BlockStart? = null, mode: MemoryOpenOptions): MemoryFileLock {
        var localOffset = offset
        if (localOffset == null) {
            localOffset = BlockStart.InvalidAddress
        }
        val compute = locks.compute(localOffset.start) { _, value ->
            val lock = value ?: ReadWriteMemoryFileLock(registry = this, start = BlockStart(offset = localOffset.start))
            lock.refs.incrementAndFetch()
            return@compute lock
        }
        return MemoryFileLock(mode, compute as ReadWriteMemoryFileLock)
    }

    fun releaseLock(offset: BlockStart, mode: MemoryOpenOptions) {
        locks.compute(offset.start) { _, value ->
            val refs = value?.refs?.decrementAndFetch() ?: 0
            if (refs == 0) {
                return@compute null
            }
            return@compute value
        }
    }

    override fun close() {
        require(locks.isEmpty())
    }
}