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

package org.wtrzcinski.memory.provider

import org.wtrzcinski.memory.MemorySegmentLedger
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.util.Require
import java.nio.file.FileStore
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileStoreAttributeView

/**
 * @see sun.nio.fs.UnixFileStore
 * @see sun.nio.fs.LinuxFileStore
 */
class MemoryFileStore(val ledger: MemorySegmentLedger) : FileStore() {

    val reservedCount: Int
        get() {
            return ledger.bitmap.reserved.count
        }

    val reservedSpaceFactor: Double
        get() {
            val reserved = ledger.bitmap.reserved.size.size.toDouble()
            val result = reserved / totalSpace
            check(result <= 1)
            return result
        }

    val metadataSpaceFactor: Double
        get() {
            val metadataSize: Double = metadata()
            return metadataSize / totalSpace
        }

    val wastedSpaceFactor: Double
        get() {
            val wastedSpaceSize: Double = wasted()
            return wastedSpaceSize / totalSpace
        }

    fun used(): DefaultBlockSize {
        return ledger.bitmap.reserved.size
    }

    fun wasted(): Double {
        return ledger.bitmap.free.findSizeSum(segmentSizeLt = ledger.headerBytes + ledger.minBodySize)
    }

    fun metadata(): Double {
        return (ledger.bitmap.reserved.count * ledger.headerBytes.size).toDouble()
    }

    override fun name(): String {
        return ledger.bitmap.toString()
    }

    override fun type(): String {
        return ledger.bitmap.toString()
    }

    override fun getUnallocatedSpace(): Long {
        return ledger.bitmap.free.size.size
    }

    override fun getTotalSpace(): Long {
        return ledger.bitmap.totalByteSize.size
    }

    override fun getUsableSpace(): Long {
        return ledger.bitmap.totalByteSize.size
    }

    override fun isReadOnly(): Boolean {
        return ledger.bitmap.isReadOnly()
    }

    override fun getAttribute(attribute: String?): Any? {
        return when (attribute) {
            "totalSpace" -> totalSpace
            "usableSpace" -> usableSpace
            "unallocatedSpace" -> unallocatedSpace
            else -> Require.unsupported()
        }
    }

    override fun supportsFileAttributeView(name: String?): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsFileAttributeView(type: Class<out FileAttributeView>): Boolean {
        TODO("Not yet implemented")
    }

    override fun <V : FileStoreAttributeView?> getFileStoreAttributeView(type: Class<V?>?): V? {
        TODO("Not yet implemented")
    }
}