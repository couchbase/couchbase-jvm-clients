/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.env

import com.couchbase.client.core
import com.couchbase.client.core.cnc.LoggingEventConsumer

case class LoggerConfig(private[scala] val customLogger: Option[LoggingEventConsumer.Logger] = None,
                        private[scala] val fallbackToConsole: Boolean = false,
                        private[scala] val disableSlf4J: Boolean = false,
                        private[scala] val loggerName: Option[String] = None) {

  private[scala] def toCore: core.env.LoggerConfig.Builder = {
    val builder = new core.env.LoggerConfig.Builder

    customLogger.foreach(v => builder.customLogger(v))
    builder.fallbackToConsole(fallbackToConsole)
    builder.disableSlf4J(disableSlf4J)
    loggerName.foreach(v => builder.loggerName(v))

    builder
  }

  def customLogger(value: LoggingEventConsumer.Logger): LoggerConfig = {
    copy(customLogger = Some(value))
  }

  def fallbackToConsole(value: Boolean): LoggerConfig = {
    copy(fallbackToConsole = value)
  }

  def disableSlf4J(value: Boolean): LoggerConfig = {
    copy(disableSlf4J = value)
  }

  def loggerName(value: String): LoggerConfig = {
    copy(loggerName = Some(value))
  }
}
