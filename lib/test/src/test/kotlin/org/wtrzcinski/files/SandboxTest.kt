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

import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.lang.foreign.*
import java.lang.invoke.MethodHandle
import java.nio.file.Files
import java.nio.file.Path

@Disabled
class SandboxTest {

    @Test
    fun should1() {
        val linker = Linker.nativeLinker()
        val stdlib: SymbolLookup = linker.defaultLookup()
        val strlen: MethodHandle = linker.downcallHandle(
            stdlib.findOrThrow("strlen"),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS)
        )

        Arena.ofConfined().use { arena ->
            val cString = arena.allocateFrom("Hello")
            val len = strlen.invokeExact(cString) as Long
            println(len)
        }
    }

    @Test
    fun should2() {
        val file = Files.createTempFile("test", ".txt")
        Files.writeString(file, "Hello")
        val link1 = Path.of("/tmp", "test.link")
        Files.deleteIfExists(link1)
        val link = Files.createSymbolicLink(link1, file)

        val readString = Files.readString(link)
        println(readString)

        Files.delete(file)
        Files.delete(link)
    }

}