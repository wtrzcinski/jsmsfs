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

package org.wtrzcinski.memory.mapper.handler

import org.wtrzcinski.memory.address.BlockSize
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer

@Suppress("UNCHECKED_CAST")
open class IterableSequenceHandler<Container : List<Element>, Element>(
    private val sizeHandler: SimpleVarHandler<DefaultBlockSize> = Int32SizeSchemaHandler,
    private val elementHandler: SimpleVarHandler<Element>,
) : HeaderBodyVarHandler<Container, DefaultBlockSize, Container> {

    fun exactBodySize(count: Int): BlockSize {
        return sizeHandler.byteSize() + elementHandler.byteSize() * count
    }

    override fun body(container: Container): Container {
        return container
    }

    override fun construct(header: DefaultBlockSize, body: Container): Container {
        return body
    }

    override fun header(data: Container): DefaultBlockSize {
        return DefaultBlockSize(data.count())
    }

    override fun bodyHandler(size: DefaultBlockSize): SimpleVarHandler<Container> {
        return object : SimpleVarHandler<Container> {
            override fun byteSize(): DefaultBlockSize {
                return size
            }

            override fun read(buffer: MemoryReadWriteBuffer): Container {
                val elementHandler = elementHandler
                val existing = mutableListOf<Element>()
                repeat(size.toInt()) {
                    val element = checkNotNull(elementHandler.read(buffer))
                    existing.add(element)
                }
                return existing as Container
            }

            override fun write(buffer: MemoryReadWriteBuffer, value: Container) {
                val elementHandler = elementHandler
                for (element in value) {
                    elementHandler.write(buffer = buffer, value = element)
                }
            }
        }
    }

    override fun headerHandler(): SimpleVarHandler<DefaultBlockSize> {
        return sizeHandler
    }

}