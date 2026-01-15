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

import org.wtrzcinski.memory.mapper.NodeMapper
import org.wtrzcinski.memory.mapper.NodeType

interface RealFilePath : FilePath {

    val node: NodeMapper

    override fun toRealPath(): RealFilePath {
        return this
    }

    override fun toAbsolutePath(): FilePath {
        return this
    }

    override fun isAbsolute(): Boolean {
        return true
    }

    override fun isDirectory(): Boolean {
        return node.readType() == NodeType.Directory
    }
}