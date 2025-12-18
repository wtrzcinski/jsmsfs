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

package org.wtrzcinski.files

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.wtrzcinski.files.Fixtures.newAlphanumericString
import org.wtrzcinski.files.memory.spi.SimpleMemoryFileStore
import java.net.URI
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

@Suppress("MayBeConstant")
internal class FilesRandomTest {
    companion object {
        val megabytes: Int = 2
        val memoryBlockSize: Int = 256
        val minStringSize: Int = memoryBlockSize
        val maxStringSize: Int = memoryBlockSize * 2
        val repeats: Int = megabytes * 1_000

        init {
            FileSystems.newFileSystem(URI.create("jsmsfs:///"), mapOf("capacity" to 1024 * 1024 * megabytes, "blockSize" to memoryBlockSize))
        }

        private fun createRandomRegularFile(parent: Path, minStringSize: Int, maxStringSize: Int): Path {
            val childName = newAlphanumericString(minLength = minStringSize, maxLength = maxStringSize)
            val child = parent.resolve(childName)
            Files.createFile(child)
            assertThat(Files.exists(child)).isTrue()
            return child
        }
    }

    @BeforeEach
    fun beforeEach() {
        val parent = Path.of(URI.create("jsmsfs:///"))

        val fileStore = parent.fileSystem.fileStores.first() as SimpleMemoryFileStore
        val used = fileStore.totalSpace - fileStore.unallocatedSpace
        assertThat(fileStore.used).isEqualTo(used)
        assertThat(used).isEqualTo(98L)
    }

    @AfterEach
    fun afterEach() {
        val parent = Path.of(URI.create("jsmsfs:///"))
        val list = Files.list(parent)
        for (child in list) {
            Files.delete(child)
        }
        val fileStore = parent.fileSystem.fileStores.first() as SimpleMemoryFileStore
        val used = fileStore.totalSpace - fileStore.unallocatedSpace
        assertThat(fileStore.used).isEqualTo(used)
        assertThat(used).isEqualTo(98L)
    }

    @Test
    fun `should create random files`() {
        val parent = Path.of(URI.create("jsmsfs:///"))
        repeat(repeats) {
            createRandomRegularFile(parent = parent, minStringSize = minStringSize, maxStringSize = maxStringSize)
        }
        
        repeat(repeats) {
            val randomChild = Files.list(parent).toList().random()
            Files.delete(randomChild)
            createRandomRegularFile(parent = parent, minStringSize = minStringSize, maxStringSize = maxStringSize)
        }

        val fileStore = parent.fileSystem.fileStores.first() as SimpleMemoryFileStore
        println(fileStore.sizeFactor)
        println(fileStore.headerSpaceFactor)
        println(fileStore.wastedSpaceFactor)
    }
}