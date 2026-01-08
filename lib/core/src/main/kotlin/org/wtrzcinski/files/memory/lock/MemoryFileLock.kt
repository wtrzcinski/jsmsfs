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

import org.wtrzcinski.files.memory.mode.Mode

@Suppress("unused")
data class MemoryFileLock(
    val mode: Mode,
    private val lock: ReadWriteMemoryFileLock? = null,
) {
    companion object {

        fun unlocked(mode: Mode): MemoryFileLock {
            return MemoryFileLock(mode)
        }

        inline fun <T> MemoryFileLock.use(block: () -> T): T {
            try {
                acquire()
                return block.invoke()
            } finally {
                release()
            }
        }
    }

    fun refCount(): Int? {
        return lock?.refCount()
    }

    fun acquire() {
        lock?.acquire(mode)
    }

    fun release() {
        lock?.release(mode)
    }
}