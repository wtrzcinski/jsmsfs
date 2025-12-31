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

@OptIn(ExperimentalContracts::class)
@Suppress("RedundantRequireNotNullCall")
object Check {

    val defensive: Boolean = Configuration.isSet(propertyName = "defensive", expectedValue = "true", matchIfMissing = true)

    inline fun isTrue(block: () -> Boolean) {
        if (defensive) {
            check(block())
        }
    }

    inline fun isNull(block: () -> Any?) {
        if (defensive) {
            check(block() == null)
        }
    }

    inline fun isNotNull(block: () -> Any?) {
        if (defensive) {
            checkNotNull(block())
        }
    }

    inline fun areInOrder(block: () -> Pair<Number, Number>) {
        if (defensive) {
            val block1 = block()
            check(block1.first.toDouble() <= block1.second.toDouble())
        }
    }

    inline fun areInOrder3(block: () -> Pair<Pair<Number, Number>, Number>) {
        if (defensive) {
            val invoke = block()

            val first = invoke.first
            val second = invoke.second

            areInOrder { first }
            areInOrder { first.second to second }
        }
    }
}