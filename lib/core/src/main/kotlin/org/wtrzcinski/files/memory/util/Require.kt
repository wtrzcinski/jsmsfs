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

import org.wtrzcinski.files.memory.exception.MemoryUnsupportedOperationException

@Suppress("NOTHING_TO_INLINE")
object Require {

    inline fun isTrue(check: Boolean) {
        require(check)
    }

    inline fun notEmpty(string: String) {
        require(string.isNotEmpty())
    }

    fun unsupported(): Nothing {
        throw MemoryUnsupportedOperationException()
    }

    fun unreachable(): Nothing {
        TODO("Not yet implemented")
    }

    fun todo(): Nothing {
        TODO("Not yet implemented")
    }
}