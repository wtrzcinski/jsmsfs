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

package org.wtrzcinski.files.arguments

import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.support.ParameterDeclarations
import org.wtrzcinski.files.Fixtures
import org.wtrzcinski.memory.allocator.MemoryScopeType
import org.wtrzcinski.memory.provider.MemoryFileSystemProvider.Companion.Capacity
import org.wtrzcinski.memory.provider.MemoryFileSystemProvider.Companion.MaxBlockSize
import org.wtrzcinski.memory.provider.MemoryFileSystemProvider.Companion.Scope
import java.net.URI
import java.nio.file.FileSystems
import java.nio.file.Files
import java.util.stream.Stream

class TestArgumentsProvider : ArgumentsProvider {
    override fun provideArguments(parameters: ParameterDeclarations, context: ExtensionContext): Stream<out Arguments> {
        val capacity = 1024 * 1024 * 10
        val blockSize = 128

        val tempFile = Files.createTempFile("jsmsfs", ".txt")
        val memoryFileSystems = MemoryScopeType
            .entries
            .filter { it != MemoryScopeType.CONFINED }
            .map {
                if (it == MemoryScopeType.PATH) {
                    Arguments.argumentSet(
                        it.name,
                        newMemoryFileSystem(
                            name = "jsmsfs:///?scope=$it",
                            env = mapOf(
                                Scope to it,
                                Capacity to capacity,
                                MaxBlockSize to blockSize,
                                "options" to "DELETE_ON_CLOSE, SPARSE",
                                "path" to tempFile,
                            )
                        )
                    )
                } else {
                    Arguments.argumentSet(
                        it.name,
                        newMemoryFileSystem(
                            name = "jsmsfs:///?scope=$it",
                            env = mapOf(
                                Scope to it,
                                Capacity to capacity,
                                MaxBlockSize to blockSize,
                            )
                        )
                    )
                }
            }
        return memoryFileSystems.stream()
    }

    private fun newMemoryFileSystem(name: String, env: Map<String, Any>): DeleteOnClosePathProvider {
        val uri = URI.create(name)
        val fileSystem = FileSystems.newFileSystem(uri, env)
        val rootDirectory = fileSystem.getPath("/")
        return DeleteOnClosePathProvider(
            delegate = SubpathPathProvider(rootDirectory),
            file = rootDirectory,
            fileSystem = fileSystem,
        )
    }

    private fun newDefaultFileSystem(): DeleteOnClosePathProvider {
        val directory = Fixtures.newTempDirectoryName()
        val tmpDirectoryPath = Files.createTempDirectory(directory)
        return DeleteOnClosePathProvider(
            delegate = SubpathPathProvider(tmpDirectoryPath),
            file = tmpDirectoryPath,
        )
    }
}