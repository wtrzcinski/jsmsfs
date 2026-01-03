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

package org.wtrzcinski.files.memory.util

import java.util.regex.PatternSyntaxException

/**
 * @see jdk.nio.zipfs.ZipUtils
 */
object RegexUtil {

    private const val regexMetaChars: String = ".^$+{[]|()"
    private const val globMetaChars: String = "\\*?[{"
    private val EOL: Char = 0.toChar() //TBD

    private fun isRegexMeta(c: Char): Boolean {
        return regexMetaChars.indexOf(c) != -1
    }

    private fun isGlobMeta(c: Char): Boolean {
        return globMetaChars.indexOf(c) != -1
    }

    private fun next(glob: String, i: Int): Char {
        if (i < glob.length) {
            return glob.get(i)
        }
        return EOL
    }

    fun toRegexPattern(globPattern: String): String {
        var inGroup = false
        val regex = StringBuilder("^")

        var i = 0
        while (i < globPattern.length) {
            var c = globPattern.get(i++)
            when (c) {
                '\\' -> {
                    // escape special characters
                    if (i == globPattern.length) {
                        throw PatternSyntaxException("No character to escape", globPattern, i - 1)
                    }
                    val next = globPattern.get(i++)
                    if (isGlobMeta(next) || isRegexMeta(next)) {
                        regex.append('\\')
                    }
                    regex.append(next)
                }

                '/' -> regex.append(c)
                '[' -> {
                    // don't match name separator in class
                    regex.append("[[^/]&&[")
                    if (next(globPattern, i) == '^') {
                        // escape the regex negation char if it appears
                        regex.append("\\^")
                        i++
                    } else {
                        // negation
                        if (next(globPattern, i) == '!') {
                            regex.append('^')
                            i++
                        }
                        // hyphen allowed at start
                        if (next(globPattern, i) == '-') {
                            regex.append('-')
                            i++
                        }
                    }
                    var hasRangeStart = false
                    var last = 0.toChar()
                    while (i < globPattern.length) {
                        c = globPattern.get(i++)
                        if (c == ']') {
                            break
                        }
                        if (c == '/') {
                            throw PatternSyntaxException("Explicit 'name separator' in class", globPattern, i - 1)
                        }
                        // TBD: how to specify ']' in a class?
                        if (c == '\\' || c == '[' || c == '&' && next(globPattern, i) == '&') {
                            // escape '\', '[' or "&&" for regex class
                            regex.append('\\')
                        }
                        regex.append(c)

                        if (c == '-') {
                            if (!hasRangeStart) {
                                throw PatternSyntaxException("Invalid range", globPattern, i - 1)
                            }
                            if ((next(globPattern, i++).also { c = it }) == EOL || c == ']') {
                                break
                            }
                            if (c < last) {
                                throw PatternSyntaxException("Invalid range", globPattern, i - 3)
                            }
                            regex.append(c)
                            hasRangeStart = false
                        } else {
                            hasRangeStart = true
                            last = c
                        }
                    }
                    if (c != ']') {
                        throw PatternSyntaxException("Missing ']", globPattern, i - 1)
                    }
                    regex.append("]]")
                }

                '{' -> {
                    if (inGroup) {
                        throw PatternSyntaxException("Cannot nest groups", globPattern, i - 1)
                    }
                    regex.append("(?:(?:")
                    inGroup = true
                }

                '}' -> if (inGroup) {
                    regex.append("))")
                    inGroup = false
                } else {
                    regex.append('}')
                }

                ',' -> if (inGroup) {
                    regex.append(")|(?:")
                } else {
                    regex.append(',')
                }

                '*' -> if (next(globPattern, i) == '*') {
                    // crosses directory boundaries
                    regex.append(".*")
                    i++
                } else {
                    // within directory boundary
                    regex.append("[^/]*")
                }

                '?' -> regex.append("[^/]")
                else -> {
                    if (isRegexMeta(c)) {
                        regex.append('\\')
                    }
                    regex.append(c)
                }
            }
        }
        if (inGroup) {
            throw PatternSyntaxException("Missing '}", globPattern, i - 1)
        }
        return regex.append('$').toString()
    }
}