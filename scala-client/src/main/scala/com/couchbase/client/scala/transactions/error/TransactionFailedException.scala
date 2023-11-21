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
package com.couchbase.client.scala.transactions.error;

import com.couchbase.client.core.cnc.events.transaction.TransactionLogEvent
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.error.transaction.internal.CoreTransactionFailedException

import scala.jdk.CollectionConverters._

/**
  * The transaction failed to reach the Committed point.
  *
  * No actors can see any changes made by this transaction.
  */
class TransactionFailedException private[scala] (
    private val internal: CoreTransactionFailedException
) extends CouchbaseException(internal.getMessage, internal.getCause, internal.context) {

  /**
    * An in-memory log is built up during each transaction.  The application may want to write this to their own logs,
    * for example upon transaction failure.
    */
  def logs(): Iterable[TransactionLogEvent] = {
    internal.logger.logs.asScala
  }

  def transactionId(): String = {
    internal.transactionId
  }
}
