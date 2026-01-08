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

package org.wtrzcinski.files.memory.lock

import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.address.DefaultBlockOffset
import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.util.Check
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.atomics.AtomicInt
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
data class ReadWriteMemoryFileLock(
    private val locks: MemoryLockRegistry,
    private val start: BlockAddress,
    private val lock: ReentrantReadWriteLock = ReentrantReadWriteLock(true),
    var refs: AtomicInt = AtomicInt(0),
) {

    init {
        Check.isTrue { start is DefaultBlockOffset }
    }

    fun refCount(): Int {
        return refs.load()
    }

    fun acquire(mode: Mode): ReadWriteMemoryFileLock {
        if (mode.open == OpenMode.Post) {
            lock.writeLock().lockInterruptibly()
        } else {
            lock.readLock().lockInterruptibly()
        }
        return this
    }

    fun release(mode: Mode) {
        if (mode.open == OpenMode.Post) {
            lock.writeLock().unlock()
        } else {
            lock.readLock().unlock()
        }

        locks.releaseLock(offset = start)
    }
}