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
import org.wtrzcinski.files.Fixtures.newAlphanumericString
import org.wtrzcinski.files.Fixtures.newRandomPath
import org.wtrzcinski.files.arguments.PathProvider
import org.wtrzcinski.files.arguments.TestArgumentsProvider
import java.nio.file.Files

@ParameterizedClass
@ArgumentsSource(TestArgumentsProvider::class)
class RegularFileStreamTest {
    @Parameter
    lateinit var pathProvider: PathProvider

    @Test
    fun `should open new input stream`() {
        val givenFilePath = pathProvider.newRandomPath()
        val givenFileContent = newAlphanumericString(lengthUntil = 1024 * 10)
        Files.writeString(givenFilePath, givenFileContent)

        val actualStream = Files.newInputStream(givenFilePath)
        val actualContent = actualStream.use {
            actualStream.readAllBytes()
        }

        assertThat(String(actualContent)).isEqualTo(givenFileContent)
    }

    @Test
    fun `should open input stream`() {
        val givenFilePath = pathProvider.newRandomPath()
        val givenFileContent = newAlphanumericString(lengthUntil = 1024 * 10)

        val outputStream = Files.newOutputStream(givenFilePath)
        outputStream.use {
            outputStream.write(givenFileContent.toByteArray(Charsets.UTF_8))
        }
        val inputStream = Files.newInputStream(givenFilePath)
        val actualContent = inputStream.use {
            inputStream.readAllBytes()
        }

        assertThat(String(actualContent, Charsets.UTF_8)).isEqualTo(givenFileContent)
    }
}