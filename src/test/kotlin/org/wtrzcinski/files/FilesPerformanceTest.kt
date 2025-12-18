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
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.provider.ArgumentsSource
import org.wtrzcinski.files.arguments.PathProvider
import org.wtrzcinski.files.arguments.TestArgumentsProvider
import org.wtrzcinski.files.Fixtures.newAlphanumericString
import org.wtrzcinski.files.Fixtures.newUniqueString
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
class FilesPerformanceTest {
    companion object {
        private const val repeats = 1000
        private const val threads = 100
    }

    @Parameter
    lateinit var fileSystem: PathProvider

    @Test
    fun `should upsert in loop`() {
        val duration = measureTime {
            repeat(repeats) {
                val givenFileName = fileSystem.getPath(newUniqueString())
                val givenFileContent = newAlphanumericString(maxLength = 512)
                Files.write(givenFileName, listOf(givenFileContent), UTF_8)
            }
        }

        println("upsert: $duration $fileSystem")
    }

    @Test
    fun `should read in loop`() {
        val all = (0..<repeats).map { _ ->
            val givenFileName = fileSystem.getPath(newUniqueString())
            val givenFileContent = newAlphanumericString(maxLength = 512)
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
        println("read: $duration $fileSystem")
    }

    @Test
    fun `should delete in loop`() {
        val all = (0..<repeats)
            .map { _ ->
                val givenFileName = fileSystem.getPath(newUniqueString())
                val givenFileContent = newAlphanumericString(maxLength = 512)
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
        println("delete $duration $fileSystem")
    }

    @Test
//    @Disabled("not going to work on any file system")
    fun `should upsert in loop parallel`() = runTest {
        val givenFileName = fileSystem.getPath(newUniqueString())

        val pool = Executors.newWorkStealingPool()
        val futures = CopyOnWriteArrayList<Future<*>>()
        val givenList = CopyOnWriteArrayList<String>()
        val actualList = CopyOnWriteArrayList<String>()
        repeat(threads) {
            val submit: Future<*> = pool.submit {
                val givenFileContent = newAlphanumericString(maxLength = 512)
                givenList.add(givenFileContent)

                Files.writeString(givenFileName, givenFileContent, UTF_8)

                val actual = Files.readString(givenFileName)
                actualList.add(actual)
            }
            futures.add(submit)
        }

        for (future in futures) {
            future.get()
        }
//        actualList.forEach { actual ->
//            assertThat(givenList).contains(actual)
//        }
    }

    @Test
    fun `should append in loop parallel`() {
        val givenFileName = fileSystem.getPath(newUniqueString())

        val pool = Executors.newWorkStealingPool()
        val futures = CopyOnWriteArrayList<Future<*>>()
        repeat(threads) {
            val submit: Future<*> = pool.submit {
                val givenFileContent = newAlphanumericString(maxLength = 512)

                Files.writeString(givenFileName, givenFileContent, APPEND, CREATE)

                val actual = Files.readString(givenFileName)
                assertThat(actual).contains(givenFileContent)
            }
            futures.add(submit)
        }

        for (future in futures) {
            future.get()
        }
    }
}