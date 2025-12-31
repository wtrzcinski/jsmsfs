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

package org.wtrzcinski.files.memory.util

import java.nio.ByteBuffer.allocate
import java.nio.channels.ReadableByteChannel
import java.nio.channels.WritableByteChannel

object IOUtil {

    fun transfer(sourceByteBuffer: ReadableByteChannel, targetByteBuffer: WritableByteChannel): Int {
        var result = 0
        val buffer = allocate(1024 * 4)
        while (true) {
            buffer.clear()
            val length = sourceByteBuffer.read(buffer)
            if (length < 0) {
                return result
            }
            result += length
            buffer.flip()
            targetByteBuffer.write(buffer)
        }
    }
}