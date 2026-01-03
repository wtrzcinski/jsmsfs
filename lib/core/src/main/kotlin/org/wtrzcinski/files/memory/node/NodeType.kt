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

package org.wtrzcinski.files.memory.node

class NodeType(val ordinal: Int) {
    companion object {
        val Regular = NodeType(0) // Bytes, Attrs, Name
        val Directory = NodeType(1) // Children Refs, Attrs, Name
        val SymbolicLink = NodeType(2) // Target Symbolic Name, Attrs, Name
        val Refs = NodeType(3) // Refs
        val String = NodeType(4) // 4 bytes for length + string bytes
        val Bytes = NodeType(5)  // bytes, can be anything

        val entries = listOf(Regular, Directory, SymbolicLink, Refs, String, Bytes)
    }
}