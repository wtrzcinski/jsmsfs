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
import org.wtrzcinski.files.memory.address.BlockAddress
import org.wtrzcinski.files.memory.buffer.ContinuousReadWriteBuffer
import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.MaxUnsignedIntInclusive
import org.wtrzcinski.files.memory.schema.ValueSchema
import org.wtrzcinski.files.memory.schema.handler.Int32AddressHandler
import org.wtrzcinski.files.memory.schema.handler.Int32SizeSchemaHandler
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
            sizeSchema = ValueSchema(Int32SizeSchemaHandler()),
            addressSchema = ValueSchema(Int32AddressHandler()),
        )
        val givenUnsignedInt = BlockAddress(Int.MAX_VALUE.toLong() * 2)

        givenByteBuffer.writeOffset(givenUnsignedInt)

        givenByteBuffer.rewind()
        val actual = givenByteBuffer.readOffset()
        assertThat(actual).isEqualTo(givenUnsignedInt)
    }
}