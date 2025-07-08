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

import com.couchbase.client.core.cnc.RequestSpan
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.core.transaction.config.CoreTransactionOptions
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory
import com.couchbase.client.scala.Collection
import com.couchbase.client.scala.util.DurationConversions

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration

/**
  * Provides all configurable parameters for a single Couchbase transaction.
  */
case class TransactionOptions(
    private val durabilityLevel: Option[DurabilityLevel] = None,
    private val parentSpan: Option[RequestSpan] = None,
    private val timeout: Option[Duration] = None,
    private val attemptContextFactory: Option[TransactionAttemptContextFactory] = None,
    private val metadataCollection: Option[CollectionIdentifier] = None
) {

  private[scala] def toCore: CoreTransactionOptions = {
    new CoreTransactionOptions(
      durabilityLevel.asJava,
      java.util.Optional.empty(),
      parentSpan.asJava,
      timeout.map(v => DurationConversions.scalaDurationToJava(v)).asJava,
      metadataCollection.asJava,
      attemptContextFactory.asJava
    )
  }

  /**
    * Overrides the default durability set, for this transaction.  The level will be used for all operations inside the transaction.
    *
    * @param durabilityLevel the durability level to set
    * @return this, for chaining
    */
  def durabilityLevel(durabilityLevel: DurabilityLevel): TransactionOptions = {
    copy(durabilityLevel = Some(durabilityLevel))
  }

  /**
    * Specifies the RequestSpan that's a parent for this transaction.
    * <p>
    * RequestSpan is a Couchbase Java SDK abstraction over an underlying tracing implementation such as OpenTelemetry
    * or OpenTracing.
    * <p>
    * @return this, for chaining
    */
  def parentSpan(parentSpan: RequestSpan): TransactionOptions = {
    copy(parentSpan = Some(parentSpan))
  }

  /**
    * Overrides the default timeout set, for this transaction.
    *
    * @return this, for chaining
    */
  def timeout(timeout: Duration): TransactionOptions = {
    copy(timeout = Some(timeout))
  }

  /**
    * Allows setting a custom collection to use for any transactional metadata documents created by this transaction.
    * <p>
    * If not set, it will default to creating these documents in the default collection of the bucket that the first
    * mutated document in the transaction is on.
    */
  def metadataCollection(collection: Collection): TransactionOptions = {
    copy(metadataCollection = Some(collection.collectionIdentifier))
  }

  private[client] def testFactory(
      attemptContextFactory: TransactionAttemptContextFactory
  ): TransactionOptions = {
    copy(attemptContextFactory = Some(attemptContextFactory))
  }
}

object TransactionOptions {
  private[scala] val Default: TransactionOptions = TransactionOptions()
}
