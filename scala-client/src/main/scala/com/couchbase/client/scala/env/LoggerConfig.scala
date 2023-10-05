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

case class LoggerConfig(
    private[scala] val fallbackToConsole: Option[Boolean] = None,
    private[scala] val disableSlf4J: Option[Boolean] = None,
    private[scala] val loggerName: Option[String] = None,
    private[scala] val diagnosticContextEnabled: Option[Boolean] = None
) {

  private[scala] def toCore: core.env.LoggerConfig.Builder = {
    val builder = new core.env.LoggerConfig.Builder

    // Even those these settings don't do anything,
    // pass them to the builder so it can log a warning.
    fallbackToConsole.foreach(v => builder.fallbackToConsole(v))
    disableSlf4J.foreach(v => builder.disableSlf4J(v))
    loggerName.foreach(v => builder.loggerName(v))

    diagnosticContextEnabled.foreach(v => builder.enableDiagnosticContext(v))

    builder
  }

  @Deprecated // This method has no effect. SLF4J is always used for logging.
  def fallbackToConsole(value: Boolean): LoggerConfig = {
    copy(fallbackToConsole = Some(value))
  }

  @Deprecated // This method has no effect. SLF4J is always used for logging.
  def disableSlf4J(value: Boolean): LoggerConfig = {
    copy(disableSlf4J = Some(value))
  }

  @Deprecated // This method has no effect. SLF4J is always used for logging.
  def loggerName(value: String): LoggerConfig = {
    copy(loggerName = Some(value))
  }

  def diagnosticContextEnabled(value: Boolean): LoggerConfig = {
    copy(diagnosticContextEnabled = Some(value))
  }

}
