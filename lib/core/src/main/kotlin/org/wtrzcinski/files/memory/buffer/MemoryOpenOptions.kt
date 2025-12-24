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

package org.wtrzcinski.files.memory.buffer

import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption
import java.nio.file.StandardOpenOption.*

data class MemoryOpenOptions(val options: Set<OpenOption>) {

    constructor(vararg options: OpenOption) : this(options.toSet())

    init {
        require(!options.contains(APPEND) || !options.contains(TRUNCATE_EXISTING))
    }

    val create: Boolean by lazy {
        options.contains(CREATE) || options.contains(StandardOpenOption.CREATE_NEW)
    }

    val createNew: Boolean by lazy {
        options.contains(StandardOpenOption.CREATE_NEW)
    }

    val write: Boolean by lazy {
        options.contains(StandardOpenOption.WRITE)
    }

    val append: Boolean by lazy {
        options.contains(APPEND)
    }

    val truncate: Boolean by lazy {
        options.contains(TRUNCATE_EXISTING) || !options.contains(APPEND)
    }

    val read: Boolean by lazy {
        options.isEmpty() || options.contains(StandardOpenOption.READ) || options.contains(StandardOpenOption.WRITE)
    }

    companion object {
        val CREATE_NEW: MemoryOpenOptions = MemoryOpenOptions(StandardOpenOption.CREATE_NEW)
        val WRITE: MemoryOpenOptions = MemoryOpenOptions(StandardOpenOption.WRITE)
        val READ: MemoryOpenOptions = MemoryOpenOptions(StandardOpenOption.READ)
    }
}