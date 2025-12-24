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

package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemoryLedger
import org.wtrzcinski.files.memory.address.ByteSize
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
internal class MemoryMapperRegistry(
    val memory: MemoryLedger,
) {
    companion object {
        val intByteSize: ByteSize = ByteSize(value = Int.SIZE_BYTES.toLong())
        val longByteSize: ByteSize = ByteSize(value = Long.SIZE_BYTES.toLong())
        val instantByteSize: ByteSize = longByteSize + intByteSize
    }

    fun startAttrs(): AttrsMapper {
        return AttrsMapper(memory = memory)
    }

    fun startNode(): NodeMapper {
        return NodeMapper(memory = memory)
    }

    fun startName(): NameMapper {
        return NameMapper(memory = memory)
    }
}