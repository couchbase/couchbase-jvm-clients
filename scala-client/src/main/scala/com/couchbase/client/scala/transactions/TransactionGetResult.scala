/**
  * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.scala.transactions;

import com.couchbase.client.core.transaction.CoreTransactionGetResult
import com.couchbase.client.scala.codec.JsonDeserializer

import scala.util.Try

/**
  * Represents a value fetched from Couchbase, along with additional transactional metadata.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.JsonDeserializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  */
case class TransactionGetResult private[scala] (
    private[scala] val internal: CoreTransactionGetResult
) {

  /** The document's id. */
  def id: String = internal.id

  /** Return the content, converted into the application's preferred representation.
g    *
    * @tparam T $SupportedTypes
    */
  def contentAs[T](implicit deserializer: JsonDeserializer[T]): Try[T] = {
    deserializer.deserialize(internal.contentAsBytes)
  }
}
