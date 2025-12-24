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

import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.provider.ArgumentsSource
import org.wtrzcinski.files.Fixtures.newAlphanumericString
import org.wtrzcinski.files.Fixtures.newRandomPath
import org.wtrzcinski.files.Fixtures.newUniqueString
import org.wtrzcinski.files.arguments.PathProvider
import org.wtrzcinski.files.arguments.TestArgumentsProvider
import org.wtrzcinski.files.memory.util.HistoricalLog
import org.wtrzcinski.files.memory.provider.MemoryFileStore
import org.wtrzcinski.files.memory.address.ByteSize
import java.nio.file.Files
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.Future
import kotlin.text.Charsets.UTF_8
import kotlin.time.measureTime

@ParameterizedClass
@ArgumentsSource(TestArgumentsProvider::class)
@Suppress("ConstPropertyName")
class PerformanceTest {
    companion object {
        private const val repeats = 100
        private const val threads = 32
    }

    @Parameter
    lateinit var pathProvider: PathProvider

    @BeforeEach
    fun beforeEach() {
        val parent = pathProvider.getPath("/")

        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore
        val used = ByteSize(fileStore.totalSpace - fileStore.unallocatedSpace)
        assertThat(fileStore.used).isEqualTo(used)
        assertThat(used).isEqualTo(ByteSize(102))
    }

    @AfterEach
    fun afterEach() {
        val parent = pathProvider.getPath("/")
        val list = Files.list(parent)
        for (child in list) {
            Files.delete(child)
        }
        val fileStore = parent.fileSystem.fileStores.first() as MemoryFileStore
        val used = ByteSize(fileStore.totalSpace - fileStore.unallocatedSpace)
        assertThat(fileStore.used).isEqualTo(used)
        assertThat(used).isEqualTo(ByteSize(102))
    }

    @Test
    fun `should upsert in loop`() {
        val duration = measureTime {
            repeat(repeats) {
                val givenFileName = pathProvider.getPath(newUniqueString())
                val givenFileContent = newAlphanumericString(lengthUntil = 256)
                Files.write(givenFileName, listOf(givenFileContent), UTF_8)
            }
        }

        HistoricalLog.debug { "upsert: $duration $pathProvider" }
    }

    @Test
    fun `should read in loop`() {
        val all = (0..<1).map { _ ->
            val givenFileName = pathProvider.getPath(newUniqueString())
            val givenFileContent = newAlphanumericString(lengthUntil = 512)
            Files.write(givenFileName, listOf(givenFileContent), UTF_8)
            val actual = Files.exists(givenFileName)
            assertThat(actual).isTrue()
            givenFileName to givenFileContent
        }

        val duration = measureTime {
            all.forEach { (givenFileName, givenFileContent) ->
                val actual = Files.readString(givenFileName)
                assertThat(actual).contains(givenFileContent)
            }
        }
        HistoricalLog.debug { "read: $duration $pathProvider" }
    }

    @Test
    fun `should delete in loop`() {
        val all = (0..<repeats)
            .map { _ ->
                val givenFileName = pathProvider.getPath(newUniqueString())
                val givenFileContent = newAlphanumericString(lengthUntil = 512)
                Files.write(givenFileName, listOf(givenFileContent), UTF_8)
                val actual = Files.exists(givenFileName)
                assertThat(actual).isTrue()
                givenFileName to givenFileContent
            }

        val duration = measureTime {
            all
                .forEach { (givenFileName, _) ->
                    Files.delete(givenFileName)
                    val actual = Files.exists(givenFileName)
                    assertThat(actual).isFalse()
                }
        }
        HistoricalLog.debug { "delete $duration $pathProvider" }
    }

    @Test
    fun `should upsert in loop parallel`() = runTest {
        val givenFileName = pathProvider.getPath(newUniqueString())

        val pool = Executors.newWorkStealingPool()
        val futures = CopyOnWriteArrayList<Future<*>>()
        val givenList = CopyOnWriteArrayList<String>()
        repeat(threads) {
            val submit: Future<*> = pool.submit {
                repeat(repeats) {
                    val givenFileContent = newAlphanumericString(lengthFrom = 128, lengthUntil = 256)
                    givenList.add(givenFileContent)

                    Files.writeString(givenFileName, givenFileContent, UTF_8)
                }
            }
            futures.add(submit)
        }
        for (future in futures) {
            future.get()
        }

        val endFile = Files.readString(givenFileName)
        val containsAny = givenList.any { endFile == it }
        assertThat(containsAny).isTrue()
    }

    @Test
    fun `should append in loop parallel`() {
        val givenFilePath = pathProvider.newRandomPath()

        val fileSystem = pathProvider.fileSystem()
        HistoricalLog.debug { fileSystem }

        val pool = Executors.newWorkStealingPool()
        val futures = CopyOnWriteArrayList<Future<*>>()
        repeat(threads) {
            val submit: Future<*> = pool.submit {
                repeat(repeats) {
                    val givenFileContent = newAlphanumericString(lengthFrom = 1, lengthUntil = 256)

                    Files.writeString(givenFilePath, givenFileContent, APPEND, CREATE)

                    val actual = Files.readString(givenFilePath)
                    assertThat(actual).contains(givenFileContent)
                }
            }
            futures.add(submit)
        }

        for (future in futures) {
            future.get()
        }
    }
}