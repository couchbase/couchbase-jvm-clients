/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.scala.manager.collection

import com.couchbase.client.core.annotation.Stability.Volatile
import com.couchbase.client.core.manager.collection.CoreCreateOrUpdateCollectionSettings
import com.couchbase.client.scala.util.DurationConversions._

import java.{lang, time}
import scala.concurrent.duration.Duration

/** Used when creating a new collection.
  *
  * @param maxExpiry is the time for the TTL for new documents in the collection.  It defaults to no expiry.
  * @param history is whether history retention override is enabled on this collection.  If not set it will default to the bucket-level setting.
  */
@Volatile
case class CreateCollectionSettings(
    private[scala] val maxExpiry: Option[Duration] = None,
    private[scala] val history: Option[Boolean] = None
) {

  /** Sets the TTL for new documents in the collection. */
  def maxExpiry(maxExpiry: Duration): CreateCollectionSettings = {
    copy(maxExpiry = Some(maxExpiry))
  }

  /** Sets whether history retention override is enabled on this collection. */
  def history(history: Boolean): CreateCollectionSettings = {
    copy(history = Some(history))
  }

  private[scala] def toCore: CoreCreateOrUpdateCollectionSettings = {
    val x = this
    new CoreCreateOrUpdateCollectionSettings {
      override def maxExpiry(): time.Duration = x.maxExpiry match {
        case Some(v) => v
        case _       => null
      }

      override def history(): lang.Boolean = x.history match {
        case Some(value) => value
        case None        => null
      }
    }
  }
}

/** Used when updating a new collection.
  *
  * @param maxExpiry is the time for the TTL for new documents in the collection.  It defaults to no expiry.
  * @param history is whether history retention override is enabled on this collection.  If not set it will default to the bucket-level setting.
  */
@Volatile
case class UpdateCollectionSettings(
    maxExpiry: Option[Duration] = None,
    history: Option[Boolean] = None
) {

  /** Sets the TTL for new documents in the collection. */
  def maxExpiry(maxExpiry: Duration): UpdateCollectionSettings = {
    copy(maxExpiry = Some(maxExpiry))
  }

  /** Sets whether history retention override is enabled on this collection. */
  def history(history: Boolean): UpdateCollectionSettings = {
    copy(history = Some(history))
  }

  private[scala] def toCore: CoreCreateOrUpdateCollectionSettings = {
    val x = this
    new CoreCreateOrUpdateCollectionSettings {
      override def maxExpiry(): time.Duration = x.maxExpiry match {
        case Some(v) => v
        case _       => null
      }

      override def history(): lang.Boolean = x.history match {
        case Some(value) => value
        case None        => null
      }
    }
  }
}
