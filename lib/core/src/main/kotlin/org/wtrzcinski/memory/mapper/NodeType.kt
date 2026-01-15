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

package org.wtrzcinski.memory.mapper

data class NodeType(val name: String, val ordinal: Int) {
    companion object {
        val Unknown = NodeType("Unknown", -1)

        val Regular = NodeType("Regular", 0) // Bytes, Attrs, Name
        val Directory = NodeType("Directory", 1) // Children Refs, Attrs, Name
        val SymbolicLink = NodeType("SymbolicLink", 2) // Target Symbolic Name, Attrs, Name

        val Bytes = NodeType("Bytes", 3)  // bytes, can be anything
        val String = NodeType("String", 4) // 4 bytes for length + string bytes
        val Refs = NodeType("Refs", 5) // Refs

        val entries: List<NodeType> = listOf(Unknown, Regular, Directory, SymbolicLink, Refs, String, Bytes)
    }
}