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

import org.wtrzcinski.memory.buffer.MemoryReadWriteBuffer

interface HeaderBodyVarHandler<Container, Header, Body> : VarHandler<Container> {

    fun headerHandler(): SimpleVarHandler<Header>

    fun bodyHandler(size: Header): SimpleVarHandler<Body>

    fun body(container: Container): Body

    fun header(data: Body): Header

    fun construct(header: Header, body: Body): Container

    override fun read(buffer: MemoryReadWriteBuffer): Container {
        val headerHandler = this.headerHandler()
        val header = headerHandler.read(buffer)
        val bodyHandler = bodyHandler(header)
        val body = bodyHandler.read(buffer)
        return construct(header, body)
    }

    override fun write(buffer: MemoryReadWriteBuffer, value: Container) {
        val body = body(value)
        val header = header( body)
        val headerHandler = headerHandler()
        headerHandler.write(buffer, header)
        val dataHandler = bodyHandler(header)
        dataHandler.write(buffer, body)
    }

}