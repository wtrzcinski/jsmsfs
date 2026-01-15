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

package org.wtrzcinski.memory.address

import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.InvalidRef

interface BlockAddress : Comparable<BlockAddress> {

    companion object {
        val InvalidAddress: DefaultBlockAddress = DefaultBlockAddress(start = InvalidRef)

        val ZeroAddress: DefaultBlockAddress = DefaultBlockAddress(start = 0L)

        operator fun invoke(ref: Long): BlockAddress {
            if (ref == InvalidAddress.start) {
                return InvalidAddress
            }
            if (ref == ZeroAddress.start) {
                return ZeroAddress
            }
            return DefaultBlockAddress(ref)
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

    operator fun plus(other: DefaultBlockSize): BlockAddress {
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