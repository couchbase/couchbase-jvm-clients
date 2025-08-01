/*
 * Copyright 2025 Couchbase, Inc.
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
package com.couchbase.client.scala.transactions.getmulti

import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.codec.Transcoder

/** A request to fetch a particular document in a getMulti operation. */
@Stability.Uncommitted
case class TransactionGetMultiSpec(
    collection: Collection,
    id: String,
    transcoder: Option[Transcoder] = None
) {

  /** Provide a transcoder so that e.g. binary documents can be handled. */
  def transcoder(t: Transcoder): TransactionGetMultiSpec =
    copy(transcoder = Some(t))
}

object TransactionGetMultiSpec {
  def get(collection: Collection, id: String): TransactionGetMultiSpec = {
    TransactionGetMultiSpec(collection, id)
  }
}
