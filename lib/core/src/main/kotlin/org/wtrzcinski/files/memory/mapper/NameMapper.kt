package org.wtrzcinski.files.memory.mapper

import org.wtrzcinski.files.memory.MemoryLedger
import org.wtrzcinski.files.memory.address.BlockStart
import org.wtrzcinski.files.memory.exception.MemoryIllegalStateException
import org.wtrzcinski.files.memory.mapper.MemoryMapperRegistry.Companion.intByteSize
import org.wtrzcinski.files.memory.util.AbstractCloseable

class NameMapper(private val memory: MemoryLedger) : BlockBodyMapper, AbstractCloseable() {
    private var name: String? = null

    fun writeName(name: String) {
        this.name = name
    }

    override fun flip(): BlockStart {
        if (tryClose()) {
            val localName = name
            if (localName == null) {
                return BlockStart.InvalidAddress
            } else {
                val newByteChannel = memory.newByteChannel(bodySize = intByteSize + (localName.length * 4))
                newByteChannel.use {
                    newByteChannel.writeString(localName)
                }
                return newByteChannel.first()
            }
        }
        throw MemoryIllegalStateException()
    }
}

