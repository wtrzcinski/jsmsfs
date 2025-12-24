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
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.provider.ArgumentsSource
import org.wtrzcinski.files.Fixtures.newAlphanumericString
import org.wtrzcinski.files.Fixtures.newUniqueString
import org.wtrzcinski.files.arguments.PathProvider
import org.wtrzcinski.files.arguments.TestArgumentsProvider
import org.wtrzcinski.files.memory.address.ByteSize
import org.wtrzcinski.files.memory.provider.MemoryFileStore
import java.nio.file.Files
import java.nio.file.StandardOpenOption

@ParameterizedClass
@ArgumentsSource(TestArgumentsProvider::class)
class RegularFileCreateTest {
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
    fun `should create file`() {
        val givenFileName = pathProvider.getPath(newUniqueString())
        val givenFileContent = "test"

        Files.writeString(givenFileName, givenFileContent, Charsets.UTF_8)

        val actual = Files.exists(givenFileName)
        assertThat(actual).isTrue()
    }

    @Test
    fun `should read file`() {
        val givenDirectoryPath = pathProvider.getPath(newAlphanumericString(lengthUntil = 10))
        val givenFilePath = givenDirectoryPath.resolve(newAlphanumericString(lengthUntil = 10))
        val givenFileContent = newAlphanumericString(lengthUntil = 512)

        Files.createDirectory(givenDirectoryPath)
        Files.writeString(givenFilePath, givenFileContent, Charsets.UTF_8)

        assertThat(Files.exists(givenDirectoryPath)).isTrue()
        assertThat(Files.exists(givenFilePath)).isTrue()
        assertThat(Files.isRegularFile(givenFilePath)).isTrue()
        assertThat(Files.readString(givenFilePath)).isEqualTo(givenFileContent)
        Files.delete(givenFilePath)
        Files.delete(givenDirectoryPath)
    }

    @Test
    fun `should create big file`() {
        val givenDirectoryPath = pathProvider.getPath(newAlphanumericString(lengthUntil = 10))
        val givenFilePath = givenDirectoryPath.resolve(newAlphanumericString(lengthUntil = 10))
        val givenFileSize = 1024 * 1024 * 4
        val givenFileContent = newAlphanumericString(lengthFrom = givenFileSize, lengthUntil = givenFileSize + 1)

        Files.createDirectory(givenDirectoryPath)
        Files.writeString(givenFilePath, givenFileContent, Charsets.UTF_8)

        assertThat(Files.exists(givenDirectoryPath)).isTrue()
        assertThat(Files.exists(givenFilePath)).isTrue()
        assertThat(Files.readString(givenFilePath)).isEqualTo(givenFileContent)
        Files.delete(givenFilePath)
        Files.delete(givenDirectoryPath)
    }

    @Test
    fun `should override file`() {
        val givenFileName = pathProvider.getPath(newUniqueString())
        val givenFileContent1 = newUniqueString()
        val givenFileContent2 = newUniqueString()

        Files.writeString(givenFileName, givenFileContent1, Charsets.UTF_8)
        Files.writeString(givenFileName, givenFileContent2, Charsets.UTF_8)

        val actual = Files.readString(givenFileName)
        assertThat(actual).isEqualTo(givenFileContent2)
    }

    @Test
    fun `should append file`() {
        val givenFileName = pathProvider.getPath(newUniqueString())
        val givenFileContent1 = newUniqueString()
        val givenFileContent2 = newUniqueString()

        Files.writeString(givenFileName, givenFileContent1, Charsets.UTF_8, StandardOpenOption.CREATE)
        Files.writeString(givenFileName, givenFileContent2, Charsets.UTF_8, StandardOpenOption.APPEND)

        val actual = Files.readString(givenFileName)
        assertThat(actual).isEqualTo(givenFileContent1 + givenFileContent2)
    }
}
