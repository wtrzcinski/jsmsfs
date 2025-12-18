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

package org.wtrzcinski.files.memory.ref

import org.wtrzcinski.files.memory.exception.MemoryIllegalArgumentException

object ByteSize {
    fun readSize(any: Any?): Long? {
        val toString = any?.toString()?.lowercase() ?: return null
        try {
            return toString.toLong()
        } catch (_: NumberFormatException) {
            if (toString.endsWith("kb")) {
                val take = toString.take(toString.length - 2)
                return take.toLong() * 1024
            } else if (toString.endsWith("mb")) {
                val take = toString.take(toString.length - 2)
                return take.toLong() * 1024 * 1024
            } else if (toString.endsWith("gb")) {
                val take = toString.take(toString.length - 2)
                return take.toLong() * 1024 * 1024 * 1024
            } else {
                throw MemoryIllegalArgumentException()
            }
        }
    }
}