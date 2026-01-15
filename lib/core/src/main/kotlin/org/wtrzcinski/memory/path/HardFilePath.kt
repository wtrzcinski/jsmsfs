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

package org.wtrzcinski.memory.path

import org.wtrzcinski.memory.MemorySegmentFileSystem
import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.mapper.NodeMapper
import org.wtrzcinski.memory.mapper.NodeType
import org.wtrzcinski.memory.util.Check

open class HardFilePath(
    override val fileSystem: MemorySegmentFileSystem,
    val ref: BlockAddress,
    override val node: NodeMapper,
    override val parent: RealFilePath,
    override val type: NodeType = node.readType(),
    override val name: String = node.readName()
) : RealFilePath {

    init {
        Check.isTrue { ref.isValid() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is HardFilePath) return false

        if (ref != other.ref) return false
        if (parent != other.parent) return false

        return true
    }

    override fun hashCode(): Int {
        var result = ref.hashCode()
        result = 31 * result + (parent?.hashCode() ?: 0)
        return result
    }
}