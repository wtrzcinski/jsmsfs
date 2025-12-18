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

import org.wtrzcinski.files.memory.channel.MemoryOpenOptions
import org.wtrzcinski.files.memory.ref.BlockStart
import java.util.concurrent.locks.ReentrantReadWriteLock

internal open class ReadWriteMemoryFileLock(
    val offset: BlockStart = BlockStart.Invalid,
    private val reentrantReadWriteLock: ReentrantReadWriteLock = ReentrantReadWriteLock(true),
) : MemoryFileLock {
    override fun acquire(mode: MemoryOpenOptions): ReadWriteMemoryFileLock {
        if (mode.write) {
            reentrantReadWriteLock.writeLock().lock()
        } else {
            reentrantReadWriteLock.readLock().lock()
        }
        return this
    }

    override fun release(mode: MemoryOpenOptions) {
        if (mode.write) {
            reentrantReadWriteLock.writeLock().unlock()
        } else {
            reentrantReadWriteLock.readLock().unlock()
        }
    }
}