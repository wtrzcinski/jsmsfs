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
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.Parameter
import org.junit.jupiter.params.ParameterizedClass
import org.junit.jupiter.params.provider.ArgumentsSource
import org.wtrzcinski.files.Fixtures.newUniqueString
import org.wtrzcinski.files.arguments.PathProvider
import org.wtrzcinski.files.arguments.TestArgumentsProvider
import org.wtrzcinski.files.memory.provider.MemoryFileStore
import java.nio.file.Files

@ParameterizedClass
@ArgumentsSource(TestArgumentsProvider::class)
class RegularFileDeleteTest {
    @Parameter
    lateinit var pathProvider: PathProvider

    @Test
    fun `should delete file`() {
        val givenFileName = pathProvider.getPath(newUniqueString())
        val givenFileContent = newUniqueString()

        Files.writeString(givenFileName, givenFileContent, Charsets.UTF_8)
        Files.delete(givenFileName)

        val actual = Files.exists(givenFileName)
        assertThat(actual).isFalse()
    }

    @Test
    fun `should delete all files`() {
        val givenRoot = pathProvider.getPath("/")
        val givenFileStore = givenRoot.fileSystem.provider().getFileStore(givenRoot) as MemoryFileStore
        val givenFile1 = givenRoot.resolve("file1-file1-file1")
        val givenFile2 = givenRoot.resolve("file2-file2-file2")
        val givenFile3 = givenRoot.resolve("file3-file3-file3")

        val reservedSizeAfterFirstFile = givenFileStore.used
        Files.createFile(givenFile1)
        Files.createFile(givenFile2)

        assertThat(Files.exists(givenRoot)).isTrue()
        assertThat(Files.exists(givenFile1)).isTrue()
        assertThat(Files.exists(givenFile2)).isTrue()

        Files.createFile(givenFile3)

//        delete file 1
        Files.delete(givenFile1)
        assertThat(Files.exists(givenRoot)).isTrue()
        assertThat(Files.exists(givenFile1)).isFalse()
        assertThat(Files.exists(givenFile2)).isTrue()
        assertThat(Files.exists(givenFile3)).isTrue()

//        delete file 2
        Files.delete(givenFile2)
        assertThat(Files.exists(givenRoot)).isTrue()
        assertThat(Files.exists(givenFile1)).isFalse()
        assertThat(Files.exists(givenFile2)).isFalse()
        assertThat(Files.exists(givenFile3)).isTrue()

//        delete file 3
        Files.delete(givenFile3)
        assertThat(Files.exists(givenRoot)).isTrue()
        assertThat(Files.exists(givenFile1)).isFalse()
        assertThat(Files.exists(givenFile2)).isFalse()
        assertThat(Files.exists(givenFile3)).isFalse()

        assertThat(givenFileStore.used).isEqualTo(reservedSizeAfterFirstFile)
    }
}