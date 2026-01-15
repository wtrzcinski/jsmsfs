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

package org.wtrzcinski.memory.mapper.schema

import org.wtrzcinski.memory.address.BlockAddress
import org.wtrzcinski.memory.address.DefaultBlockSize
import org.wtrzcinski.memory.mapper.handler.*

class SchemaRegistry(
    val sizeHandler: SimpleVarHandler<DefaultBlockSize>,
    val refHandler: SimpleVarHandler<BlockAddress>,
    val listSizeHandler: SimpleVarHandler<DefaultBlockSize> = sequenceSizeHandler(sizeHandler),
    val stringHandler: StringVarHandler = StringVarHandler(listSizeHandler),
    val permissionsHandler: PermissionsVarHandler = PermissionsVarHandler(),
    val refListHandler: RefListVarHandler = RefListVarHandler(listSizeHandler, refHandler),
) {
    val instantHandler: InstantHandler = InstantHandler()
    val nodeSchema: NodeStructSchema = NodeStructSchema.nodeSchema(this)

    companion object {
        operator fun invoke(memoryByteSize: Long): SchemaRegistry {
            val refHandler = IntAddressHandler(memoryByteSize = memoryByteSize)
            val sizeHandler = IntSizeHandler(memoryByteSize = memoryByteSize)
            return SchemaRegistry(
                sizeHandler = sizeHandler,
                refHandler = refHandler,
            )
        }

        private fun sequenceSizeHandler(sizeHandler: SimpleVarHandler<DefaultBlockSize>): SimpleVarHandler<DefaultBlockSize> {
            return if (sizeHandler.byteSize() < Int32SizeSchemaHandler.byteSize()) {
                sizeHandler
            } else {
                Int32SizeSchemaHandler
            }
        }

    }
}