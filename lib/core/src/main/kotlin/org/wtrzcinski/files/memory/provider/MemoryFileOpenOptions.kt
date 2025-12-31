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

package org.wtrzcinski.files.memory.provider

import org.wtrzcinski.files.memory.mode.Mode
import org.wtrzcinski.files.memory.mode.OpenMode
import org.wtrzcinski.files.memory.mode.WriteMode
import org.wtrzcinski.files.memory.util.Check
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption

data class MemoryFileOpenOptions(val options: Set<OpenOption>) {

    companion object {
        val REQUIRE_NEW: MemoryFileOpenOptions = MemoryFileOpenOptions(StandardOpenOption.CREATE_NEW)

        val WRITE_TRUNCATE: MemoryFileOpenOptions = MemoryFileOpenOptions(StandardOpenOption.WRITE)

        val READ: MemoryFileOpenOptions = MemoryFileOpenOptions(StandardOpenOption.READ)
    }

    constructor(vararg options: OpenOption) : this(options.toSet())

    init {
        Check.isTrue { !options.contains(StandardOpenOption.APPEND) || !options.contains(StandardOpenOption.TRUNCATE_EXISTING) }
    }

    val mode: Mode
        get() {
            val openMode = if (readWrite) {
                OpenMode.ReadWrite
            } else if (read) {
                OpenMode.ReadOnly
            } else {
                OpenMode.Close
            }
            val writeMode = if (requireNew) {
                WriteMode.RequireNew
            } else if (append) {
                WriteMode.AppendToExisting
            } else {
                WriteMode.TruncateExisting
            }
            return Mode(
                open = openMode,
                write = writeMode,
            )
        }

    val create: Boolean by lazy {
        allowNew || requireNew
    }

    val allowNew: Boolean by lazy {
        options.contains(StandardOpenOption.CREATE)
    }

    val requireNew: Boolean by lazy {
        options.contains(StandardOpenOption.CREATE_NEW)
    }

    val readWrite: Boolean by lazy {
        options.contains(StandardOpenOption.WRITE)
    }

    val append: Boolean by lazy {
        val result = options.contains(StandardOpenOption.APPEND)
        result
    }

    val truncate: Boolean by lazy {
        options.contains(StandardOpenOption.TRUNCATE_EXISTING) || !options.contains(StandardOpenOption.APPEND)
    }

    val read: Boolean by lazy {
        options.isEmpty() || options.contains(StandardOpenOption.READ) || options.contains(StandardOpenOption.WRITE)
    }
}