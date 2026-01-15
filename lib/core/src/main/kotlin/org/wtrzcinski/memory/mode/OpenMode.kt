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

package org.wtrzcinski.memory.mode

import java.nio.file.OpenOption

enum class OpenMode(
    val create: Boolean  = false,
    val update: Boolean  = false,
    val read: Boolean = false,
    val open: Boolean = create || update || read,
    val safe : Boolean = open && !create && !update,
    val idempotent: Boolean = open && !create,
) : OpenOption {

    Post(create = true, update = true, read = true) {
        override fun next(): Sequence<OpenMode> {
            return sequenceOf(Put)
        }
    },

    Put(update = true, read = true) {
        override fun next(): Sequence<OpenMode> {
            return sequenceOf(Get, Unlock)
        }
    },

    Get(read = true) {
        override fun next(): Sequence<OpenMode> {
            return sequenceOf(Put, Unlock)
        }
    },

    Unlock {
        override fun next(): Sequence<OpenMode> {
            return sequenceOf(Delete)
        }
    },

    Delete {
        override fun next(): Sequence<OpenMode> {
            return sequenceOf()
        }
    };

    abstract fun next(): Sequence<OpenMode>
}