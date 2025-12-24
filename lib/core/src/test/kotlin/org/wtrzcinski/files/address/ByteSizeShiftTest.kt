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

package org.wtrzcinski.files.address

import org.assertj.core.api.Assertions.assertThat
import org.wtrzcinski.files.memory.address.ByteSizeShift
import kotlin.test.Test

class ByteSizeShiftTest {
    @Test
    fun `should convert`() {
        assertThat(ByteSizeShift.noop.convert(value = 42L)).isEqualTo(42)
        assertThat(ByteSizeShift.kb.convert(value = 42L)).isEqualTo(42 * 1024L)
        assertThat(ByteSizeShift.mb.convert(value = 42L)).isEqualTo(42 * 1024L * 1024L)
        assertThat(ByteSizeShift.gb.convert(value = 42L)).isEqualTo(42 * 1024L * 1024L * 1024L)
    }
}