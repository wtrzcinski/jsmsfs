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

package org.wtrzcinski.files.memory.address

import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.InvalidRef

interface BlockAddress : Comparable<BlockAddress> {

    companion object {
        val InvalidOffset: DefaultBlockOffset = DefaultBlockOffset(start = InvalidRef)

        val EmptyOffset: DefaultBlockOffset = DefaultBlockOffset(start = 0L)

        operator fun invoke(offset: Long): BlockAddress {
            if (offset == InvalidOffset.start) {
                return InvalidOffset
            }
            if (offset == EmptyOffset.start) {
                return EmptyOffset
            }
            return DefaultBlockOffset(offset)
        }
    }

    val start: Long

    fun isValid(): Boolean {
        return start != InvalidRef
    }

    operator fun plus(other: BlockAddress): BlockAddress {
        return invoke(Math.addExact(this.start, other.start))
    }

    operator fun plus(other: BlockSize): BlockAddress {
        return invoke(Math.addExact(this.start, other.size))
    }

    operator fun plus(other: ByteSize): BlockAddress {
        return invoke(Math.addExact(this.start, other.size))
    }

    operator fun plus(other: Long): BlockAddress {
        return invoke(Math.addExact(this.start, other))
    }

    override fun compareTo(other: BlockAddress): Int {
        val compareTo = start.compareTo(other.start)
        if (compareTo != 0) {
            return compareTo
        }
        if (this is BlockSize && other is BlockSize) {
            return size.compareTo(other.size)
        }
        return 0
    }

}