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

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.reflect.KClass

@Suppress("unused")
object HistoricalLog {

    private val collect: Boolean = Configuration.isSet("log.logger", "collect", matchIfMissing = false)

    private val debug: Boolean = Configuration.isSet("log.logger", "debug", matchIfMissing = false)

    private val info: Boolean = true

    data class LogEntry(
        val type: KClass<*>,
        val threadName: String,
        val time: Long,
        val message: Any
    ) {
        override fun toString(): String {
            return "$threadName $time $message"
        }
    }

    private val loggers: ConcurrentHashMap<String, org.slf4j.Logger> = ConcurrentHashMap()

    private val map = ConcurrentHashMap<String, CopyOnWriteArrayList<LogEntry>>()

    fun info(target: Any, message: () -> Any) {
        if (info) {
            val loggerName = checkNotNull(target::class.simpleName)
            val log = loggers.computeIfAbsent(loggerName) {
                LoggerFactory.getLogger(loggerName)
            }
            log.info("{}", message.invoke())
        }
    }

    fun debug(target: Any, message: () -> Any) {
        if (collect) {
            val threadName = Thread.currentThread().name
            map.compute(threadName) { _, value ->
                if (value == null) {
                    val list = CopyOnWriteArrayList<LogEntry>()
                    list.add(entry(target::class, threadName, message))
                    return@compute list
                } else {
                    value.add(entry(target::class, threadName, message))
                    return@compute value
                }
            }
        } else if (debug) {
            val loggerName = checkNotNull(target::class.simpleName)
            val log = loggers.computeIfAbsent(loggerName) {
                LoggerFactory.getLogger(loggerName)
            }
            log.debug("{}", message.invoke())
        }
    }

    private fun entry(type: KClass<*>, threadName: String, message: () -> Any): LogEntry {
        val now = System.nanoTime()

        return LogEntry(type, threadName, now, message.invoke())
    }

    fun getAndClear(): List<Any> {
        synchronized(map) {
            val result = map.values
                .flatten()
                .sortedBy { it.time }
                .toList()
            for (entry in map.entries) {
                entry.value.clear()
            }
            return result
        }
    }
}