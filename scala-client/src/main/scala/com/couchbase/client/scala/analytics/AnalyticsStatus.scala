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
package com.couchbase.client.scala.analytics

sealed trait AnalyticsStatus

object AnalyticsStatus {

  case object Running extends AnalyticsStatus

  case object Success extends AnalyticsStatus

  case object Errors extends AnalyticsStatus

  case object Completed extends AnalyticsStatus

  case object Stopped extends AnalyticsStatus

  case object Timeout extends AnalyticsStatus

  case object Closed extends AnalyticsStatus

  case object Fatal extends AnalyticsStatus

  case object Aborted extends AnalyticsStatus

  case object Unknown extends AnalyticsStatus

  private[scala] def from(in: String): AnalyticsStatus = {
    in.toLowerCase match {
      case "running" => Running
      case "success" => Success
      case "errors" => Errors
      case "completed" => Completed
      case "stopped" => Stopped
      case "timeout" => Timeout
      case "closed" => Closed
      case "fatal" => Fatal
      case "aborted" => Aborted
      case _ => Unknown
    }
  }
}