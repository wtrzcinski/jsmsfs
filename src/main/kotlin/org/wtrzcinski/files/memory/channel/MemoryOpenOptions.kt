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

package org.wtrzcinski.files.memory.channel

import java.nio.file.StandardOpenOption

data class MemoryOpenOptions(val options: Set<StandardOpenOption>) {

    constructor(vararg options: StandardOpenOption) : this(options.toSet())

    val create: Boolean
        get() {
            return options.contains(StandardOpenOption.CREATE) || createNew
        }

    val createNew: Boolean
        get() {
            return options.contains(StandardOpenOption.CREATE_NEW)
        }

    val write: Boolean
        get() {
            return options.contains(StandardOpenOption.WRITE) || append || truncateExisting
        }

    val append: Boolean
        get() {
            return options.contains(StandardOpenOption.APPEND)
        }

    val truncateExisting: Boolean
        get() {
            return options.contains(StandardOpenOption.TRUNCATE_EXISTING)
        }

    val read: Boolean
        get() {
            return options.isEmpty()
                    || options.contains(StandardOpenOption.READ)
                    || options.contains(StandardOpenOption.WRITE)
        }

    companion object {
        val CREATE_NEW = MemoryOpenOptions(StandardOpenOption.CREATE_NEW)
        val WRITE = MemoryOpenOptions(StandardOpenOption.WRITE)
        val READ = MemoryOpenOptions(StandardOpenOption.READ)
    }
}