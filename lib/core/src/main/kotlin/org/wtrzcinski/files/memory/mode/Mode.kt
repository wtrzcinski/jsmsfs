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

package org.wtrzcinski.files.memory.mode

data class Mode(
    val open: OpenMode,
    val write: WriteMode,
    val read: ReadMode = ReadMode.Block,
) {
    companion object {
        fun readOnly(): Mode {
            return of(true)
        }

        fun readWrite(): Mode {
            return of(false)
        }

        fun of(readOnly: Boolean): Mode {
            return if (readOnly) {
                Mode(OpenMode.ReadOnly, WriteMode.UseExisting, ReadMode.Block)
            } else {
                Mode(OpenMode.ReadWrite, WriteMode.RequireNew, ReadMode.Block)
            }
        }
    }
}