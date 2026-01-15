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

package org.wtrzcinski.files

import org.assertj.core.api.Assertions
import org.wtrzcinski.memory.mapper.NodeType
import org.wtrzcinski.memory.path.HardFilePath
import org.wtrzcinski.memory.provider.MemoryFilePathAdapter
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random

class Registry {
    val regular = ConcurrentHashMap<Path, String>()
    val links = ConcurrentHashMap<Path, Path>()
    val directories = CopyOnWriteArrayList<Path>()

    fun count(): Int {
        return regular.size + directories.size + links.size
    }

    fun addDirectory(root: Path) {
        directories.add(root)
    }

//    @Synchronized
    fun checkRandomFile() {
        val entry = regular.entries.randomOrNull()
        if (entry != null) {
            val actual = Files.readString(entry.key)
            Assertions.assertThat(actual).isEqualTo(entry.value)
        }
    }

//    @Synchronized
    fun checkRandomLink() {
        val entry = links.entries.randomOrNull()
        if (entry != null) {
            val link = entry.key
            val file = entry.value
            if (Files.exists(file)) {
                val fileContent = Files.readString(file)
                val actual = Files.readString(link)
                Assertions.assertThat(actual).isEqualTo(fileContent)
            } else {
                val exception = Assertions.catchException { Files.readString(link) }
                Assertions.assertThat(exception).isInstanceOf(NoSuchFileException::class.java)
            }
        }
    }

//    @Synchronized
    fun deleteRandom() {
        val randomParent: Path = directories.random()
        deleteRandom(randomParent)
    }

    private tailrec fun deleteRandom(directory: Path): Boolean {
        require(directory is MemoryFilePathAdapter)
        val delegate = directory.delegate
        if (delegate is HardFilePath) {
            val node = delegate.node
            require(node.readType() == NodeType.Directory)
        }

        val children = Files.list(directory).toList()
        if (children.isEmpty()) {
            require(directories.remove(directory))
            Files.delete(directory)
            return true
        } else {
            val file = children.random()
            require(file is MemoryFilePathAdapter)
            require(file.delegate is HardFilePath)

            if (Files.isDirectory(file)) {
                return deleteRandom(directory = file)
            } else if (Files.isRegularFile(file)) {
                val value = regular.remove(file)
                requireNotNull(value)
                Files.delete(file)
                return true
            } else if (Files.isSymbolicLink(file)) {
                requireNotNull(links.remove(file))
                Files.delete(file)
                return true
            }
        }

        TODO("Not yet implemented")
    }

//    @Synchronized
    fun createRandom() {
        val parent = directories.random()
        val nextInt = Random.nextInt(from = 0, until = 4)
        if (nextInt == 0) {
            createRandomDirectory(parent = parent)
        } else if (nextInt == 1) {
            createRandomLink(parent = parent)
        } else {
            createRandomRegularFile(parent = parent)
        }
    }

    private fun createRandomRegularFile(parent: Path) {
        val childName = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val childContent = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val child: Path = parent.resolve(childName)
        val createFile = Files.createFile(child)
        Files.writeString(createFile, childContent, StandardOpenOption.WRITE)
        Assertions.assertThat(Files.exists(createFile)).isTrue()
        require(createFile is MemoryFilePathAdapter)
        regular[createFile.toRealPath()] = childContent
    }

    private fun createRandomDirectory(parent: Path) {
        val childName = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val child = parent.resolve(childName)
        val createDirectory = Files.createDirectory(child)
        Assertions.assertThat(Files.exists(createDirectory)).isTrue()
        require(createDirectory is MemoryFilePathAdapter)
        directories.add(createDirectory.toRealPath())
    }

    private fun createRandomLink(parent: Path) {
        val childName = Fixtures.newAlphanumericString(
            lengthFrom = MonkeyTest.minStringSize,
            lengthUntil = MonkeyTest.maxStringSize
        )
        val child = parent.resolve(childName)
        val randomFile = regular.keys.randomOrNull()
        if (randomFile != null) {
            val createSymbolicLink = Files.createSymbolicLink(child, randomFile)
            Assertions.assertThat(Files.exists(createSymbolicLink)).isTrue()
            require(createSymbolicLink is MemoryFilePathAdapter)
            links[createSymbolicLink.toRealPath()] = randomFile
        }
    }
}