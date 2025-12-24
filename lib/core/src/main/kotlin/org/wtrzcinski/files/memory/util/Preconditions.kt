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

package org.wtrzcinski.files.memory.util

import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract

@OptIn(ExperimentalContracts::class)
object Preconditions {

    val assert: Boolean = Configuration.isSet(propertyName = "preconditions.debug", expectedValue = "true", matchIfMissing = true)

    fun assertTrue(value: Boolean) {
        if (assert) {
            require(value)
        }
    }

    inline fun assertTrue(block: () -> Boolean) {
        if (assert) {
            requireTrue(block())
        }
    }

    inline fun assertNull(block: () -> Any?) {
        if (assert) {
            require(block() == null)
        }
    }

    fun requireTrue(value: Boolean) {
        contract {
            returns() implies value
        }
        require(value)
    }
}