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
import org.wtrzcinski.files.Fixtures.tmpdir
import org.wtrzcinski.files.arguments.PathProvider
import org.wtrzcinski.files.arguments.TestArgumentsProvider
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.PosixFileAttributes

@ParameterizedClass
@ArgumentsSource(TestArgumentsProvider::class)
class FilesMoveTest {
    @Parameter
    lateinit var pathProvider: PathProvider

    @Test
    fun `should move file`() {
        val givenFileName1 = pathProvider.getPath(newUniqueString(), newUniqueString())
        val givenFileName2 = pathProvider.getPath(newUniqueString(), newUniqueString())
        val givenFileContent = newUniqueString()

        Files.createDirectories(givenFileName1.parent)
        Files.writeString(givenFileName1, givenFileContent, Charsets.UTF_8, StandardOpenOption.CREATE)
        Files.createDirectories(givenFileName2.parent)
        Files.move(givenFileName1, givenFileName2)

        assertThat(Files.exists(givenFileName1)).isFalse()
        assertThat(Files.readString(givenFileName2)).isEqualTo(givenFileContent)
    }

    @Test
    fun `should move file to different file system`() {
        val givenSourceFile = pathProvider.getPath(newUniqueString())
        val givenTargetFile = Path.of("$tmpdir/${newUniqueString()}.data")
        val givenFileContent = newUniqueString()
        Files.writeString(givenSourceFile, givenFileContent, Charsets.UTF_8, StandardOpenOption.CREATE)
        val givenSourceAttrs = Files.readAttributes(givenSourceFile, PosixFileAttributes::class.java)

        Thread.sleep(100L)
        Files.move(givenSourceFile, givenTargetFile)

        val givenTargetAttrs = Files.readAttributes(givenTargetFile, PosixFileAttributes::class.java)
        assertThat(Files.exists(givenSourceFile)).isFalse()
        assertThat(Files.readString(givenTargetFile)).isEqualTo(givenFileContent)
        assertThat(givenSourceAttrs.lastAccessTime()).isEqualTo(givenTargetAttrs.lastAccessTime())
        assertThat(givenSourceAttrs.lastModifiedTime()).isEqualTo(givenTargetAttrs.lastModifiedTime())
        assertThat(givenSourceAttrs.creationTime()).isNotEqualTo(givenTargetAttrs.creationTime())
        assertThat(givenSourceAttrs.permissions().toSortedSet()).isEqualTo(givenTargetAttrs.permissions().toSortedSet())
        Files.delete(givenTargetFile)
    }

    @Test
    fun `should move file from different file system`() {
        val givenSourceFile = Path.of("$tmpdir/${newUniqueString()}.data")
        val givenTargetFile = pathProvider.getPath(newUniqueString())
        val givenFileContent = newUniqueString()
        Files.writeString(givenSourceFile, givenFileContent, Charsets.UTF_8, StandardOpenOption.CREATE)
        val givenSourceAttrs = Files.readAttributes(givenSourceFile, PosixFileAttributes::class.java)

        Thread.sleep(100L)
        Files.move(givenSourceFile, givenTargetFile)

        val givenTargetAttrs = Files.readAttributes(givenTargetFile, PosixFileAttributes::class.java)
        assertThat(Files.exists(givenSourceFile)).isFalse()
        assertThat(Files.readString(givenTargetFile)).isEqualTo(givenFileContent)
        assertThat(givenSourceAttrs.lastAccessTime()).isEqualTo(givenTargetAttrs.lastAccessTime())
        assertThat(givenSourceAttrs.lastModifiedTime()).isEqualTo(givenTargetAttrs.lastModifiedTime())
        assertThat(givenSourceAttrs.creationTime()).isNotEqualTo(givenTargetAttrs.creationTime())
        assertThat(givenSourceAttrs.permissions().toSortedSet()).isEqualTo(givenTargetAttrs.permissions().toSortedSet())
        Files.delete(givenTargetFile)
    }
}