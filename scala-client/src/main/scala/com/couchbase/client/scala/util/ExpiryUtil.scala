/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.scala.util

import java.time.Instant

import scala.concurrent.duration.Duration

object ExpiryUtil {

  /** Converts expiration times into an absolute epoch timestamp in seconds. */
  def expiryActual(
      expiry: Duration,
      expiryTime: Option[Instant],
      now: Instant = Instant.now
  ): Long = {
    expiryTime match {
      case Some(et) => et.getEpochSecond
      case _ =>
        expiry match {
          case null        => 0
          case x: Duration => now.getEpochSecond + x.toSeconds
        }
    }
  }

}
