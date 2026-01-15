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

import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler
import org.wtrzcinski.memory.mapper.handler.VarHandler

interface ValueSchema<T> : Schema {

    companion object {
        operator fun <T> invoke(handler: SimpleVarHandler<T>): ValueSchema<T> {
            return object : ValueSchema<T> {
                override val handler: SimpleVarHandler<T> = handler
            }
        }
    }

    val handler: VarHandler<T>

}