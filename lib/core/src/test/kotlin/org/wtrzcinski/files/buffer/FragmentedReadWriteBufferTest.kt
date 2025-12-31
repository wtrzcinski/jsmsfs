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
import org.wtrzcinski.files.memory.MemorySegmentContext
import org.wtrzcinski.files.memory.address.ByteSize
import java.time.Instant

class FragmentedReadWriteBufferTest {
    companion object {
        private val context = MemorySegmentContext(capacity = ByteSize.readSize("4mb"))

        val ledger = context.ledger
    }

    @Test
    fun `should flip buffer`() {
        val instants = (0..<1024 * 128).map { Instant.now() }
        val tmpBuffer = ledger.newBuffer(bodyAlignment = ByteSize(29))
        instants.forEach {
            tmpBuffer.writeInstant(it)
        }
        assertThat(tmpBuffer.count()).isEqualTo(386)

        tmpBuffer.flip()
        val bodySize = tmpBuffer.remaining()
        val directBuffer = ledger.newBuffer(bodyAlignment = bodySize)
        directBuffer.write(value = tmpBuffer)
        directBuffer.flip()

        instants.forEach {
            assertThat(directBuffer.readInstant()).isEqualTo(it)
        }
        tmpBuffer.close()
        directBuffer.close()
    }
}