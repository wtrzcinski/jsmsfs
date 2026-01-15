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

package org.wtrzcinski.memory.mapper.handler

import org.wtrzcinski.memory.address.DefaultBlockSize

/**
 * @see java.lang.invoke.VarHandle
 */
interface SimpleVarHandler<T>: VarHandler<T> {
    companion object {
        val byteSize: DefaultBlockSize = DefaultBlockSize(value = Byte.SIZE_BYTES.toLong())

        val shortByteSize: DefaultBlockSize = DefaultBlockSize(value = Short.SIZE_BYTES.toLong())

        val intByteSize: DefaultBlockSize = DefaultBlockSize(value = Int.SIZE_BYTES.toLong())

        val longByteSize: DefaultBlockSize = DefaultBlockSize(value = Long.SIZE_BYTES.toLong())

        val instantByteSize: DefaultBlockSize = longByteSize + intByteSize

        const val InvalidRef: Long = -1

        val MaxUnsignedShortInclusive: Long = java.lang.Short.toUnsignedLong(-1) - 1L

        val UnsignedShortRange: LongRange = 0..MaxUnsignedShortInclusive

        val MaxUnsignedIntInclusive: Long = Integer.toUnsignedLong(-1) - 1L

        val UnsignedIntRange: LongRange = 0..MaxUnsignedIntInclusive
    }

    fun byteSize(): DefaultBlockSize

}