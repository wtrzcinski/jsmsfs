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

package org.wtrzcinski.files.memory.node

import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermissions
import java.time.Instant

class AttributesBlock(
    val now: Instant = Instant.ofEpochSecond(0, 0),
    val lastAccessTime: Instant = now,
    val lastModifiedTime: Instant = now,
    val creationTime: Instant = now,
    val permissions: Set<PosixFilePermission> = PosixFilePermissions.fromString("rwx".repeat(3)),
    val owner: String = "",
    val group: String = "",
)