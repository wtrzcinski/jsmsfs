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

package org.wtrzcinski.files.memory.provider

import org.wtrzcinski.files.memory.MemorySegmentLedger
import org.wtrzcinski.files.memory.address.ByteSize
import java.nio.file.FileStore
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileStoreAttributeView

class MemoryFileStore(val ledger: MemorySegmentLedger) : FileStore() {

    val reservedCount: Int
        get() {
            return ledger.bitmap.reserved.count
        }

    val reservedSpaceFactor: Double
        get() {
            val reserved = ledger.bitmap.reserved.size.size.toDouble()
            val total = ledger.bitmap.totalByteSize
            val result = reserved / total.size
            check(result <= 1)
            return result
        }

    val metadataSpaceFactor: Double
        get() {
            val metadataSize: Double = (ledger.bitmap.reserved.count * ledger.headerBytes.size).toDouble()
            return metadataSize / ledger.bitmap.reserved.size.toDouble()
        }

    val wastedSpaceFactor: Double
        get() {
            val wastedSpaceSize: Double = ledger.bitmap.free.findSizeSum(segmentSizeLt = ledger.headerBytes)
            return wastedSpaceSize / ledger.bitmap.reserved.size.toDouble()
        }

    val used: ByteSize get() {
        return ledger.bitmap.reserved.size
    }

    override fun name(): String {
        return ledger.bitmap.toString()
    }

    override fun type(): String {
        return ledger.bitmap.toString()
    }

    override fun getTotalSpace(): Long {
        return ledger.bitmap.totalByteSize.size
    }

    override fun getUnallocatedSpace(): Long {
        return ledger.bitmap.free.size.size
    }

    override fun getUsableSpace(): Long {
        return ledger.bitmap.totalByteSize.size
    }

    override fun isReadOnly(): Boolean {
        return ledger.bitmap.isReadOnly()
    }

    override fun supportsFileAttributeView(type: Class<out FileAttributeView>): Boolean {
        TODO("Not yet implemented")
    }

    override fun supportsFileAttributeView(name: String?): Boolean {
        TODO("Not yet implemented")
    }

    override fun <V : FileStoreAttributeView?> getFileStoreAttributeView(type: Class<V?>?): V? {
        TODO("Not yet implemented")
    }

    override fun getAttribute(attribute: String?): Any? {
        TODO("Not yet implemented")
    }
}