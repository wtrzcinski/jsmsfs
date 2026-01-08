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

package org.wtrzcinski.files.memory.mode

import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.util.Check
import java.io.Closeable
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
open class ModeMonitor(
    mode: Mode = Mode.create(),
) : Closeable {

    private val openModeAtomic = AtomicReference(mode.open)

    private val writeModeAtomic = AtomicReference(mode.write)

    val openMode: OpenMode get() = openModeAtomic.load()

    val writeMode: WriteMode get() = writeModeAtomic.load()

    val mode: Mode get() = Mode(openMode, writeMode)

    fun isOpen(): Boolean {
        return openMode.open
    }

    fun isClosed(): Boolean {
        return !openMode.open
    }

    fun isSafe(): Boolean {
        return openMode.safe
    }

    fun isIdempotent(): Boolean {
        return openMode.idempotent
    }

    fun isCreating(): Boolean {
        return openMode.create
    }

    fun isUpdating(): Boolean {
        return openMode.update
    }

    fun isDeleting(): Boolean {
        return openMode == OpenMode.Delete
    }

    fun isReading(): Boolean {
        return openMode.read
    }

    override fun close() {
        if (!tryClose()) {
            throwIllegalStateException()
        }
    }

    fun tryRelease(): Boolean {
        tryFlip()
        tryClose()
        return exchangeMode(prevValue = OpenMode.Unlock, nextValue = OpenMode.Delete)
    }

    fun tryClose(): Boolean {
        tryFlip()
        return exchangeMode(prevValue = OpenMode.Put, nextValue = OpenMode.Unlock)
    }

    fun tryFlip(nextValue: OpenMode = OpenMode.Put): Boolean {
        return exchangeMode(prevValue = OpenMode.Post, nextValue = OpenMode.Put)
    }

    private fun exchangeMode(prevValue: OpenMode, nextValue: OpenMode): Boolean {
        Check.isTrue { prevValue.next().contains(nextValue) }

        return openModeAtomic.compareAndSet(expectedValue = prevValue, newValue = nextValue)
    }

    fun throwIfNotOpen() {
        if (!isOpen()) {
            throwIllegalStateException()
        }
    }

    fun throwIfNotClosed() {
        if (!isClosed()) {
            throwIllegalStateException()
        }
    }

    fun throwIfNotWritable() {
        throwIfNotOpen()
        if (!isCreating() && !isUpdating()) {
            throwIllegalStateException()
        }
    }

    fun throwIfNotReadable() {
        throwIfNotOpen()
        if (!isReading()) {
            throwIllegalStateException()
        }
    }

    fun throwIfDeleting() {
        if (isDeleting()) {
            throwIllegalStateException()
        }
    }

    fun throwIllegalStateException(): Nothing {
        throw MemoryIllegalStateException()
    }
}