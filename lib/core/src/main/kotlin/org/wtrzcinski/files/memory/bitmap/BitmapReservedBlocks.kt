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

package org.wtrzcinski.files.memory.bitmap

import org.wtrzcinski.files.memory.exception.OptimisticLockException
import org.wtrzcinski.files.memory.address.ByteSize
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.atomics.minusAssign
import kotlin.concurrent.atomics.plusAssign

@OptIn(ExperimentalAtomicApi::class)
class BitmapReservedBlocks {

    private val reserved: CopyOnWriteArrayList<BitmapEntry> = CopyOnWriteArrayList()

    private val first: CopyOnWriteArrayList<BitmapEntry> = CopyOnWriteArrayList()

    private val reservedSize: AtomicLong = AtomicLong(0)

    val count: Int get() = reserved.size

    val byteSize: ByteSize get() = ByteSize(reservedSize.load())

    fun add(other: BitmapEntry): BitmapEntry {
        reserved.add(other)
        if (other.isFirst()) {
            first.add(other)
        }
        this.reservedSize += other.size
        return other
    }

    fun remove(other: BitmapEntry) {
        val remove = reserved.remove(other)
        if (!remove) {
            throw OptimisticLockException()
        }
        if (other.isFirst()) {
            first.remove(other)
        }
        this.reservedSize -= other.size
    }

    fun copy(): Iterable<BitmapEntry> {
        return ArrayList(reserved)
    }
}