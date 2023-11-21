/*
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
package com.couchbase.client.scala.transactions.config;

import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory
import com.couchbase.client.scala.Collection

/**
  * Allows configuring a single-query-transaction.
  */
case class SingleQueryTransactionOptions private (
    private val durabilityLevel: Option[DurabilityLevel] = None,
    private val attemptContextFactory: Option[TransactionAttemptContextFactory] = None,
    private val metadataCollection: Option[CollectionIdentifier] = None
) {

  /**
    * Overrides the default durability level set, for this transaction.
    *
    * @param durabilityLevel the durability level to set
    * @return this, for chaining
    */
  def durabilityLevel(durabilityLevel: DurabilityLevel): SingleQueryTransactionOptions = {
    copy(durabilityLevel = Some(durabilityLevel))
  }

  /**
    * Allows setting a custom collection to use for any transactional metadata documents created by this transaction.
    * <p>
    * If not set, it will default to creating these documents in the default collection of the bucket that the first
    * mutated document in the transaction is on.
    */
  private[client] def metadataCollection(collection: Collection): SingleQueryTransactionOptions = {
    copy(metadataCollection = Some(collection.collectionIdentifier))
  }

  /**
    * For internal testing.  Applications should not require this.
    */
  private[client] def testFactory(
      attemptContextFactory: TransactionAttemptContextFactory
  ): SingleQueryTransactionOptions = {
    copy(attemptContextFactory = Some(attemptContextFactory))
  }
}
