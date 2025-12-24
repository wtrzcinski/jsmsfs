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

package org.wtrzcinski.files.arguments

import java.nio.file.FileSystem
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.isDirectory

class DeleteOnClosePathProvider(
    private val delegate: PathProvider,
    private val file: Path? = null,
    private val fileSystem: FileSystem? = null,
) : PathProvider by delegate {
    companion object {
        private fun Path.deleteRecursively() {
            if (this.isDirectory()) {
                Files.list(this).forEach { sub ->
                    sub.deleteRecursively()
                }
            }
            Files.delete(this)
        }
    }

    override fun close() {
        try {
            delegate.close()
        } finally {
            fileSystem.use {
                file?.deleteRecursively()
            }
        }
    }

    override fun toString(): String {
        return "${javaClass.simpleName}(file=$file, fileSystem=$fileSystem, delegate=$delegate)"
    }
}