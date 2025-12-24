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

package org.wtrzcinski.files.memory.node.directory

import org.wtrzcinski.files.memory.node.MemoryPath
import java.nio.channels.SeekableByteChannel
import java.nio.file.*
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView

internal class MemorySecureDirectoryStream(
    private val parent: MemoryPath,
    private val filter: DirectoryStream.Filter<in Path>,
    private val delegate: DirectoryStream<Path> = MemoryDirectoryStream(parent, filter),
) : DirectoryStream<Path> by delegate, SecureDirectoryStream<Path> {

    override fun newDirectoryStream(path: Path, vararg options: LinkOption): SecureDirectoryStream<Path> {
        TODO("Not yet implemented")
    }

    override fun newByteChannel(path: Path, options: Set<OpenOption>, vararg attrs: FileAttribute<*>): SeekableByteChannel {
        TODO("Not yet implemented")
    }

    override fun deleteFile(path: Path) {
        val absolutePath = parent.resolve(path)
        require(filter.accept(absolutePath))
        val provider = parent.fileSystem.provider()
        provider.delete(absolutePath)
    }

    override fun deleteDirectory(path: Path) {
        val absolutePath = parent.resolve(path)
        require(filter.accept(absolutePath))
        val provider = parent.fileSystem.provider()
        provider.delete(absolutePath)
    }

    override fun move(srcpath: Path?, targetdir: SecureDirectoryStream<Path>, targetpath: Path?) {
        TODO("Not yet implemented")
    }

    override fun <V : FileAttributeView?> getFileAttributeView(type: Class<V>): V? {
        TODO("Not yet implemented")
    }

    override fun <V : FileAttributeView?> getFileAttributeView(path: Path, type: Class<V>, vararg options: LinkOption): V? {
        TODO("Not yet implemented")
    }
}