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

package org.wtrzcinski.files.memory.spi

import org.wtrzcinski.files.memory.bitmap.BitmapRegistryGroup
import org.wtrzcinski.files.memory.block.MemoryBlockRegistry
import java.nio.file.FileStore
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileStoreAttributeView

internal class SimpleMemoryFileStore(
    private val bitmapStore: BitmapRegistryGroup,
    private val blockStore: MemoryBlockRegistry,
) : FileStore() {
    val sizeFactor: Double
        get() {
            val reserved = bitmapStore.reserved.size.toDouble()
            val result = reserved / bitmapStore.totalByteSize
            require(result <= 1)
            return result
        }

    val headerSpaceFactor: Double
        get() {
            val metadataSize: Double = (bitmapStore.reserved.count * blockStore.headerSize).toDouble()
            return metadataSize / bitmapStore.reserved.size
        }

    val wastedSpaceFactor: Double
        get() {
            val wastedSpaceSize = bitmapStore.free.findSizeSum(segmentSizeLt = blockStore.minMemoryBlockSize)
            return wastedSpaceSize / bitmapStore.reserved.size
        }

    val used: Long get() = bitmapStore.reserved.size

    override fun name(): String {
        return bitmapStore.toString()
    }

    override fun type(): String {
        return bitmapStore.toString()
    }

    override fun getTotalSpace(): Long {
        return bitmapStore.totalByteSize
    }

    override fun getUnallocatedSpace(): Long {
        return bitmapStore.free.size
    }

    override fun getUsableSpace(): Long {
        return bitmapStore.totalByteSize
    }

    override fun isReadOnly(): Boolean {
        return bitmapStore.isReadOnly()
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