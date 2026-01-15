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

data class Mode(
    val open: OpenMode,
    val write: UnsafeMode,
    val read: SafeMode,
) {
    companion object {
        fun create(): Mode {
            return Mode(open = OpenMode.Post, write = UnsafeMode.RequireNew, read = SafeMode.All)
        }

        fun update(): Mode {
            return Mode(open = OpenMode.Put, write = UnsafeMode.RequireExisting, read = SafeMode.All)
        }

        fun read(): Mode {
            return Mode(open = OpenMode.Get, write = UnsafeMode.RequireExisting, read = SafeMode.All)
        }
    }
}