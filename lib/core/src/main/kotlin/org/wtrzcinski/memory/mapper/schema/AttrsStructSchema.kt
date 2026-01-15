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
import java.nio.file.attribute.PosixFilePermission
import java.time.Instant

class AttrsStructSchema(private val delegate: StructSchema) {
    companion object {
        private val lastAccessTime: PropertyDefinition<Instant> = PropertyDefinition(Instant::class, "lastAccessTime")

        private val lastModifiedTime: PropertyDefinition<Instant> = PropertyDefinition(Instant::class, "lastModifiedTime")

        private val creationTime: PropertyDefinition<Instant> = PropertyDefinition(Instant::class, "creationTime")

        private val permissions = PropertyDefinition<Set<PosixFilePermission>>(Set::class, "permissions")

        private val owner: PropertyDefinition<String> = PropertyDefinition(String::class, "owner")

        private val group: PropertyDefinition<String> = PropertyDefinition(String::class, "group")

        fun attrsSchema(schemas: SchemaRegistry): AttrsStructSchema = AttrsStructSchema(
            StructSchema.builder()
                .property(lastAccessTime, handler = schemas.instantHandler)
                .property(lastModifiedTime, handler = schemas.instantHandler)
                .property(creationTime, handler = schemas.instantHandler)
                .property(permissions, handler = schemas.permissionsHandler)
                .property(owner, handler = schemas.stringHandler, maxBodySize = DefaultBlockSize(4 * 100))
                .property(group, handler = schemas.stringHandler, maxBodySize = DefaultBlockSize(4 * 100))
                .build()
        )
    }

    val lastAccessTime = delegate.property(Companion.lastAccessTime)

    val lastModifiedTime = delegate.property(Companion.lastModifiedTime)

    val creationTime = delegate.property(Companion.creationTime)

    val permissions = delegate.property(Companion.permissions)

    val owner = delegate.property(Companion.owner)

    val group = delegate.property(Companion.group)

    val offsetRange: ClosedRange<BlockAddress> = delegate.addressRange

    fun maxBodySize(): DefaultBlockSize {
        return delegate.maxBodySize()
    }
}