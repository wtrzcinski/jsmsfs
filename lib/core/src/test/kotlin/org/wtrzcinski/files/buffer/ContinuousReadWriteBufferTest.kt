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

package org.wtrzcinski.files.buffer

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.buffer.ContinuousReadWriteBuffer
import org.wtrzcinski.memory.mapper.handler.Int32AddressHandler
import org.wtrzcinski.memory.mapper.handler.Int32SizeSchemaHandler
import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.MaxUnsignedIntInclusive
import org.wtrzcinski.memory.mapper.schema.SchemaRegistry
import java.lang.foreign.MemorySegment

internal class ContinuousReadWriteBufferTest {

    @Test
    fun `max unsigned int should be valid`() {
        assertThat(MaxUnsignedIntInclusive).isEqualTo(Int.MAX_VALUE.toLong() * 2)
    }

    @Test
    fun `should store ref as unsigned int`() {
        val givenMemorySegment = MemorySegment.ofArray(ByteArray(4))
        val givenByteBuffer = ContinuousReadWriteBuffer(
            memorySegment = givenMemorySegment,
            schemas = SchemaRegistry(
                sizeHandler = Int32SizeSchemaHandler,
                refHandler = Int32AddressHandler,
            )
        )
        val givenUnsignedInt = BlockAddress(Int.MAX_VALUE.toLong() * 2)

        givenByteBuffer.writeRef(givenUnsignedInt)

        givenByteBuffer.rewind()
        val actual = givenByteBuffer.readRef()
        assertThat(actual).isEqualTo(givenUnsignedInt)
    }
}