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

import com.couchbase.client.core.api.query.CoreQueryScanConsistency
import com.couchbase.client.core.io.CollectionIdentifier
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds
import com.couchbase.client.core.transaction.cleanup.{CleanerFactory, ClientRecordFactory}
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory
import com.couchbase.client.scala.query.QueryScanConsistency
import com.couchbase.client.scala.transactions.TransactionKeyspace
import com.couchbase.client.scala.transactions.error.TransactionExpiredException
import com.couchbase.client.scala.transactions.internal.TransactionsSupportedExtensionsUtil
import com.couchbase.client.scala.util.DurationConversions.{javaDurationToScala, _}

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration

/**
  * Provides all configurable parameters for Couchbase transactions.
  */
case class TransactionsConfig(
    private val cleanupConfig: Option[TransactionsCleanupConfig] = None,
    private val durabilityLevel: Option[DurabilityLevel] = None,
    private val timeout: Option[Duration] = None,
    private val metadataCollection: Option[CollectionIdentifier] = None,
    private val attemptContextFactory: Option[TransactionAttemptContextFactory] = None,
    private val cleanerFactory: Option[CleanerFactory] = None,
    private val clientRecordFactory: Option[ClientRecordFactory] = None,
    private val queryConfig: Option[TransactionsQueryConfig] = None
) {

  /**
    * Configures transaction cleanup.
    */
  def cleanupConfig(config: TransactionsCleanupConfig): TransactionsConfig = {
    copy(cleanupConfig = Some(config))
  }

  /**
    * Sets the maximum time that transactions can run for.  The default is 15 seconds.
    * After this time, the transaction will abort.  Note that this could be mid-commit, in which case the cleanup process
    * will complete the transaction asynchronously at a later point.
    * <p>
    * Applications can increase or decrease this as desired.  The trade-off to understand is that documents that are
    * being mutated in a transaction A, are effectively locked from being updated by other transactions until
    * transaction A has completed - committed or rolled back.  If transaction A is unable to complete for whatever
    * reason, the document can be locked for this <code>timeout</code> time.
    */
  def timeout(timeout: Duration): TransactionsConfig = {
    copy(timeout = Some(timeout))
  }

  /**
    * All transaction writes will be performed with this durability setting.
    * <p>
    * The default setting is DurabilityLevel.MAJORITY, meaning a transaction will pause on each write
    * until it is available in-memory on a majority of configured replicas.
    * <p>
    * DurabilityLevel.NONE is not supported and provides no ACID transactional guarantees.
    */
  def durabilityLevel(durabilityLevel: DurabilityLevel): TransactionsConfig = {
    copy(durabilityLevel = Some(durabilityLevel))
  }

  /**
    * Allows setting a custom collection to use for any transactional metadata documents.
    * <p>
    * If not set, it will default to creating these documents in the default collection of the bucket that the first
    * mutated document in the transaction is on.
    * <p>
    * This collection will be added to the set of collections being cleaned up.
    */
  def metadataCollection(collection: TransactionKeyspace): TransactionsConfig = {
    copy(metadataCollection = Some(collection.toCollectionIdentifier))
  }

  /**
    * Sets the default query configuration for all transactions.
    *
    * @param queryConfig the query configuration to use
    * @return this, for chaining
    */
  def queryConfig(queryConfig: TransactionsQueryConfig): TransactionsConfig = {
    copy(queryConfig = Some(queryConfig))
  }

  private[client] def testFactory(
      attemptContextFactory: TransactionAttemptContextFactory,
      cleanerFactory: CleanerFactory
  ): TransactionsConfig = {
    copy(attemptContextFactory = Some(attemptContextFactory), cleanerFactory = Some(cleanerFactory))
  }

  private[client] def toCore: CoreTransactionsConfig = {
    val to: java.time.Duration = timeout
      .map(v => scalaDurationToJava(v))
      .getOrElse(CoreTransactionsConfig.DEFAULT_TRANSACTION_TIMEOUT)

    new CoreTransactionsConfig(
      durabilityLevel.getOrElse(CoreTransactionsConfig.DEFAULT_TRANSACTION_DURABILITY_LEVEL),
      to,
      cleanupConfig.getOrElse(TransactionsCleanupConfig()).toCore,
      attemptContextFactory.getOrElse(new TransactionAttemptContextFactory()),
      cleanerFactory.getOrElse(new CleanerFactory()),
      clientRecordFactory.getOrElse(new ClientRecordFactory()),
      ActiveTransactionRecordIds.NUM_ATRS_DEFAULT,
      metadataCollection.asJava,
      queryConfig
        .flatMap(
          v =>
            v.scanConsistency.map {
              case QueryScanConsistency.NotBounded => CoreQueryScanConsistency.NOT_BOUNDED.toString
              case _                               => CoreQueryScanConsistency.REQUEST_PLUS.toString
            }
        )
        .asJava,
      TransactionsSupportedExtensionsUtil.Supported
    );
  }
}
