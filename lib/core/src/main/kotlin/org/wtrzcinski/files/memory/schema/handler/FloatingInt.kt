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

package org.wtrzcinski.files.memory.schema.handler

import org.wtrzcinski.files.memory.schema.ValueHandler.Companion.InvalidRef

// todo wojtek reinventing BigInteger here
class FloatingInt(
    val bitCount: Int,
    val value: Long,
) {
    companion object {
        fun roundToBytes(maxValue: Long): Int {
            val exp = Math.getExponent(maxValue.toDouble())
            val mod = exp.mod(8)
            if (mod == 0) {
                return (exp.toLong() / 8).toInt()
            }
            return (exp.toLong() / 8 + 1).toInt()
        }

        fun intToBytes(bitCount: Int, value: Long): ByteArray {
            var l = value
            val result = ByteArray(bitCount)
            for (i in (bitCount - 1) downTo 0) {
                result[i] = (l and 0xFFL).toByte()
                l = l shr 8
            }
            return result
        }

        fun bytesToInt(bitCount: Int, byteArray: ByteArray): Long? {
            if (byteArray.all { it == InvalidRef.toByte() }) {
                return null
            }
            var result: Long = 0
            for (i in 0..<bitCount) {
                result = result shl 8
                result = result or (byteArray[i].toInt() and 0xFF).toLong()
            }
            return result
        }
    }
}