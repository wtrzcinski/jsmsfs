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

package org.wtrzcinski.files.memory.util

import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import java.io.Closeable
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
abstract class AbstractCloseable : Closeable {

    private val closed = AtomicBoolean(false)

    fun isOpen(): Boolean {
        return !closed.load()
    }

    override fun close() {
        if (!tryClose()) {
            throwIsClosed()
        }
    }

    protected fun tryClose(): Boolean {
        return closed.compareAndSet(expectedValue = false, newValue = true)
    }

    protected fun checkIsOpen() {
        if (!isOpen()) {
            throwIsClosed()
        }
    }

    protected fun throwIsClosed(): Nothing {
        throw MemoryIllegalStateException()
    }
}