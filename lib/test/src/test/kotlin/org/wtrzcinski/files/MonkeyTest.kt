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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.allocator.MemoryScopeType.GLOBAL
import org.wtrzcinski.files.memory.provider.MemoryFilePathAdapter.Companion.deleteRecursively
import org.wtrzcinski.files.memory.provider.MemoryFileStore
import org.wtrzcinski.files.memory.provider.MemoryFileSystemProvider
import org.wtrzcinski.files.memory.provider.MemoryFileSystemProvider.Companion.Capacity
import org.wtrzcinski.files.memory.provider.MemoryFileSystemProvider.Companion.Scope
import java.net.URI
import java.nio.file.Path
import java.util.concurrent.TimeUnit

@Suppress("MayBeConstant")
internal class MonkeyTest {

    companion object {

        private val log = LoggerFactory.getLogger(MonkeyTest::class.java)

        val capacity: String = "4MB"
        val minStringSize: Int = 256
        val maxStringSize: Int = 512
        val repeats: Int = 2000 * 60

        private val registry = Registry()

        init {
            MemoryFileSystemProvider.newFileSystem(
                uri = URI.create("jsmsfs:///"),
                env = mapOf(
                    Scope to GLOBAL,
                    Capacity to capacity,
//                    MaxBlockSize to 128,
                ),
            )
            val root = Path.of(URI.create("jsmsfs:///"))
            registry.addDirectory(root)
        }
    }

    @BeforeEach
    fun beforeEach() {
        val parent = Path.of(URI.create("jsmsfs:///"))

        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore
        val used = ByteSize(fileStore.totalSpace - fileStore.unallocatedSpace)
        Assertions.assertThat(fileStore.used).isEqualTo(used)
        Assertions.assertThat(used).isEqualTo(ByteSize(97))
    }

    @AfterEach
    fun afterEach() {
        val parent = Path.of(URI.create("jsmsfs:///"))
        parent.deleteRecursively()

        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore
        val used = ByteSize(fileStore.totalSpace - fileStore.unallocatedSpace)
        Assertions.assertThat(fileStore.used).isEqualTo(used)
        Assertions.assertThat(used).isEqualTo(ByteSize(97))
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    fun `should create random files`() {
        val root = Path.of(URI.create("jsmsfs:///"))
        val fileStore = root.fileSystem.fileStores.first() as MemoryFileStore

        repeat(repeats) {
            while (fileStore.reservedSpaceFactor > 0.9) {
                registry.deleteRandom()
            }
            registry.createRandom()
            registry.checkRandomFile()
            registry.checkRandomLink()
        }

        log.info("files {} + {} + {} = {}", registry.regular.size, registry.directories.size, registry.links.size, registry.count())
        log.info("fragments: {}", fileStore.reservedCount)
        log.info("reserved space {}", fileStore.reservedSpaceFactor)
        log.info("metadata space {}", fileStore.metadataSpaceFactor)
        log.info("wasted space {}", fileStore.wastedSpaceFactor)
    }

}