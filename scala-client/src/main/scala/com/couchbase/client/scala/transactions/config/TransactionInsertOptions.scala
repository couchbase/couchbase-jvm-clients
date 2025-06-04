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
package com.couchbase.client.scala.transactions.config

import com.couchbase.client.core.annotation.{SinceCouchbase, Stability}
import com.couchbase.client.scala.codec.Transcoder

/**
  * Operations controlling a transactional insert.
  */
@Stability.Volatile
case class TransactionInsertOptions private (transcoder: Option[Transcoder] = None) {

  /**
    * Specify a custom [[Transcoder]] that is used to encode the content of the document.
    *
    * If not-specified, the [[com.couchbase.client.scala.env.ClusterEnvironment]]'s [[com.couchbase.client.scala.codec.JsonSerializer]]
    * (NOT transcoder) is used.
    *
    * It is marked as being available from 7.6.2 because prior to this, only JSON documents were supported in transactions.  This release added
    * support for binary documents.
    *
    * @param transcoder the custom transcoder that should be used for encoding.
    * @return a copy of this with the change applied, for chaining.
    */
  @SinceCouchbase("7.6.2")
  def transcoder(transcoder: Transcoder): TransactionInsertOptions = {
    copy(transcoder = Some(transcoder))
  }
}

object TransactionInsertOptions {
  val Default: TransactionInsertOptions = TransactionInsertOptions()
}
