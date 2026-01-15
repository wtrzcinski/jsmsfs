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

import org.wtrzcinski.memory.mapper.handler.SimpleVarHandler.Companion.InvalidRef

class UnsignedInt(byteCount: Int, val value: Long) {
    companion object {
        fun roundToBytes(maxValue: Long): Int {
            val exp = Math.getExponent(maxValue.toDouble())
            val mod = exp.mod(8)
            val byteCount = if (mod == 0) {
                (exp.toLong() / 8).toInt()
            } else {
                (exp.toLong() / 8 + 1).toInt()
            }
//            val valueOf = BigInteger.valueOf(maxValue)
//            val toByteArray = valueOf.toByteArray()
//            val bitLen = valueOf.bitLength()
//            val bitCount = valueOf.bitCount()
//            require((bitLen / 8) == result)
            return byteCount
        }

        fun intToBytes(byteCount: Int, value: Long): ByteArray {
            var l = value
            val result = ByteArray(byteCount)
            for (index in (byteCount - 1) downTo 0) {
                result[index] = (l and 0xFFL).toByte()
                l = l shr 8
            }
            return result
        }

        fun bytesToInt(byteCount: Int, bytes: ByteArray): Long? {
            if (bytes.all { it == InvalidRef.toByte() }) {
                return null
            }
            var result: Long = 0
            for (index in 0..<byteCount) {
                result = result shl 8
                result = result or (bytes[index].toInt() and 0xFF).toLong()
            }
            return result
        }
    }
}