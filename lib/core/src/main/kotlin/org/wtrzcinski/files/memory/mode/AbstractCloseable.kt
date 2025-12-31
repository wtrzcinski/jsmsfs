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

package org.wtrzcinski.files.memory.mode

import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.util.Check
import java.io.Closeable
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi

@OptIn(ExperimentalAtomicApi::class)
@Suppress("unused")
open class AbstractCloseable(
    mode: Mode = Mode.readWrite(),
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

    fun isReadOnly(): Boolean {
        return openMode == OpenMode.ReadOnly
    }

    fun isReleased(): Boolean {
        return openMode == OpenMode.Release
    }

    fun isWritable(): Boolean {
        return openMode.writeable
    }

    fun isReadable(): Boolean {
        return openMode.readable
    }

    override fun close() {
        if (!tryClose()) {
            throwIllegalStateException()
        }
    }

    protected fun tryRelease(): Boolean {
        tryFlip()
        tryClose()
        return moveMode(prevValue = OpenMode.Close, nextValue = OpenMode.Release)
    }

    fun tryClose(): Boolean {
        tryFlip()
        return moveMode(prevValue = OpenMode.ReadOnly, nextValue = OpenMode.Close)
    }

    protected fun tryFlip(): Boolean {
        return moveMode(prevValue = OpenMode.ReadWrite, nextValue = OpenMode.ReadOnly)
    }

    private fun moveMode(prevValue: OpenMode, nextValue: OpenMode): Boolean {
        Check.isTrue { prevValue.ordinal == nextValue.ordinal - 1 }

        return openModeAtomic.compareAndSet(expectedValue = prevValue, newValue = nextValue)
    }

    protected fun checkIsOpen() {
        if (!isOpen()) {
            throwIllegalStateException()
        }
    }

    protected fun checkIsClosed() {
        if (!isClosed()) {
            throwIllegalStateException()
        }
    }

    protected fun checkIsWritable() {
        checkIsOpen()
        if (!isWritable()) {
            throwIllegalStateException()
        }
    }

    protected fun checkIsReadable() {
        checkIsOpen()
        if (!isReadable()) {
            throwIllegalStateException()
        }
    }

    protected fun throwIllegalStateException(): Nothing {
        throw MemoryIllegalStateException()
    }
}