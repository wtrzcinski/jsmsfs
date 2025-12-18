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

import org.wtrzcinski.files.arguments.PathProvider
import java.io.File
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

object Fixtures {

    val tmpdir = System.getProperty("java.io.tmpdir")

    private val alphanumeric: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')

    private val counter = AtomicInteger(0)

    fun newUniqueString(): String {
        val millis = System.currentTimeMillis()
        val counter = counter.incrementAndGet()
        return "test-string-$millis-$counter"
    }

    fun newTempDirectoryName(): String {
        val millis = System.currentTimeMillis()
        val counter = counter.incrementAndGet()
        val directory = "test-dir-$millis-$counter"
        return directory
    }

    fun newAlphanumericString(minLength: Int = 1, maxLength: Int): String {
        require(minLength < maxLength)

        val length: Int = Random.nextInt(minLength, maxLength)
        val chars: List<Char> = (0..<length)
            .map { Random.nextInt(alphanumeric.size) }
            .map { alphanumeric[it] }
        return chars.joinToString("")
    }

    fun PathProvider.newRandomPath(): Path {
        return getPath(newUniqueString())
    }

    fun PathProvider.getRootPath(): Path {
        return getPath(File.separator)
    }
}