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

package org.wtrzcinski.files.monkey

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.wtrzcinski.files.Fixtures
import org.wtrzcinski.files.memory.util.HistoricalLog
import org.wtrzcinski.files.memory.provider.MemoryFileStore
import org.wtrzcinski.files.memory.provider.MemoryFileSystemProvider
import org.wtrzcinski.files.memory.address.ByteSize
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.TimeUnit

@Suppress("MayBeConstant")
internal class MonkeyTest {
    companion object {
        val megabytes: Int = 4
        val blockSize: Int = 256
        val minStringSize: Int = blockSize
        val maxStringSize: Int = blockSize * 2
        val repeats: Int = 5_000

        init {
            MemoryFileSystemProvider.newFileSystem(
                uri = URI.create("jsmsfs:///"),
                env = mapOf("scope" to "SHARED", "capacity" to "${megabytes}MB", "blockSize" to blockSize)
            )
        }

        private fun createRandomRegularFile(parent: Path): Path {
            val childName = Fixtures.newAlphanumericString(lengthFrom = minStringSize, lengthUntil = maxStringSize)
            val childContent = Fixtures.newAlphanumericString(lengthFrom = minStringSize, lengthUntil = maxStringSize)
            val child = parent.resolve(childName)
            val createFile = Files.createFile(child)
            Files.writeString(createFile, childContent, StandardOpenOption.WRITE)
            Assertions.assertThat(Files.exists(createFile)).isTrue()
            return child
        }

        private fun createRandomDirectory(parent: Path): Path {
            val childName = Fixtures.newAlphanumericString(lengthFrom = minStringSize, lengthUntil = maxStringSize)
            val child = parent.resolve(childName)
            val createDirectory = Files.createDirectory(child)
            Assertions.assertThat(Files.exists(createDirectory)).isTrue()
            return createDirectory
        }
    }

    @BeforeEach
    fun beforeEach() {
        val parent = Path.of(URI.create("jsmsfs:///"))

        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore
        val used = ByteSize(fileStore.totalSpace - fileStore.unallocatedSpace)
        Assertions.assertThat(fileStore.used).isEqualTo(used)
        Assertions.assertThat(used).isEqualTo(ByteSize(102))
    }

    @AfterEach
    fun afterEach() {
        val parent = Path.of(URI.create("jsmsfs:///"))
        val list = Files.list(parent)
        for (child in list) {
            Files.delete(child)
        }
        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore
        val used = ByteSize(fileStore.totalSpace - fileStore.unallocatedSpace)
        Assertions.assertThat(fileStore.used).isEqualTo(used)
        Assertions.assertThat(used).isEqualTo(ByteSize(102))
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    fun `should create random files`() {
        val parent = Path.of(URI.create("jsmsfs:///"))
        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore

        repeat(repeats) {
            if (fileStore.reservedSpaceFactor <= 0.9) {
                createRandomRegularFile(parent = parent)
            } else {
                val list = Files.list(parent).toList()
                val randomChild = list.random()
                Files.delete(randomChild)
            }
        }

        HistoricalLog.debug({ fileStore.reservedCount })
        HistoricalLog.debug({ fileStore.reservedSpaceFactor })
        HistoricalLog.debug({ fileStore.metadataSpaceFactor })
        HistoricalLog.debug({ fileStore.wastedSpaceFactor })
    }
}