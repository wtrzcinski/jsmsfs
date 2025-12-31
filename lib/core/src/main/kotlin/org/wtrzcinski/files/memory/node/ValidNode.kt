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

import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.address.DefaultBlockStart
import org.wtrzcinski.files.memory.util.Check.isTrue

sealed class ValidNode(
    val fileType: NodeType,
    val offset: DefaultBlockStart,
    val dataRef: DefaultBlockStart = BlockStart.InvalidAddress,
    val attrsRef: DefaultBlockStart = BlockStart.InvalidAddress,
    val nameRef: DefaultBlockStart = BlockStart.InvalidAddress,
    override val name: String,
) : NamedNode {

    init {
        isTrue { offset.isValid() }
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(fileType=$fileType, nodeRef=$offset, name='$name')"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ValidNode

        if (fileType != other.fileType) return false
        if (offset != other.offset) return false
        if (name != other.name) return false

        return true
    }

    override fun hashCode(): Int {
        var result = fileType.hashCode()
        result = 31 * result + offset.hashCode()
        result = 31 * result + name.hashCode()
        return result
    }
}