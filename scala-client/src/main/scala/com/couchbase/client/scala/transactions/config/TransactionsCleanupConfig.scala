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
import com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig
import com.couchbase.client.scala.transactions.TransactionKeyspace
import com.couchbase.client.scala.util.DurationConversions.scalaDurationToJava

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/**
  * Provides all configurable parameters for Couchbase transactions cleanup.
  */
case class TransactionsCleanupConfig private[scala] (
    private val cleanupClientAttempts: Option[Boolean] = None,
    private val cleanupLostAttempts: Option[Boolean] = None,
    private val cleanupWindow: Option[Duration] = None,
    private val cleanupSet: Option[Set[TransactionKeyspace]] = None
) {

  /**
    * Controls where a background thread is created to cleanup any transaction attempts made by this client.
    * <p>
    * The default is true and users should generally not change this: cleanup is an essential part of Couchbase
    * transactions.
    */
  def cleanupClientAttempts(cleanupClientAttempts: Boolean): TransactionsCleanupConfig = {
    copy(cleanupClientAttempts = Some(cleanupClientAttempts))
  }

  /**
    * Controls where a background process is created to cleanup any 'lost' transaction attempts.
    * <p>
    * The default is true and users should generally not change this: cleanup is an essential part of Couchbase
    * transactions.
    */
  def cleanupLostAttempts(cleanupLostAttempts: Boolean): TransactionsCleanupConfig = {
    copy(cleanupLostAttempts = Some(cleanupLostAttempts))
  }

  /**
    * Part of the lost attempts background cleanup process.  Specifies the window during which the cleanup
    * process is sure to discover all lost transactions.
    * <p>
    * The default setting of 60 seconds is tuned to balance how quickly such transactions are discovered, while
    * minimising impact on the cluster.  If the application would prefer to discover
    * lost transactions more swiftly, but at the cost of increased impact, it can feel free to reduce this
    * parameter.
    */
  def cleanupWindow(cleanupWindow: Duration): TransactionsCleanupConfig = {
    copy(cleanupWindow = Some(cleanupWindow))
  }

  /**
    * Adds collections to the set of metadata collections that will be cleaned up automatically.
    * <p>
    * Collections will be added automatically to this 'cleanup set' as transactions are performed, so generally
    * an application will not need to change this.
    * <p>
    * Setting this parameter will also start cleanup immediately rather than on the first transaction.
    */
  def collections(collections: Set[TransactionKeyspace]): TransactionsCleanupConfig = {
    copy(cleanupSet = Some(collections))
  }

  private[scala] def toCore: CoreTransactionsCleanupConfig = {
    val cw: java.time.Duration = cleanupWindow
      .map(v => scalaDurationToJava(v))
      .getOrElse(CoreTransactionsCleanupConfig.DEFAULT_TRANSACTION_CLEANUP_WINDOW)

    val cs: java.util.Set[CollectionIdentifier] = cleanupSet
      .map(v => v.map(x => x.toCollectionIdentifier).asJava)
      .getOrElse(new java.util.HashSet[CollectionIdentifier]())

    new CoreTransactionsCleanupConfig(
      cleanupLostAttempts.getOrElse(
        CoreTransactionsCleanupConfig.DEFAULT_TRANSACTIONS_CLEANUP_LOST_ENABLED
      ),
      cleanupClientAttempts.getOrElse(
        CoreTransactionsCleanupConfig.DEFAULT_TRANSACTIONS_CLEANUP_CLIENT_ENABLED
      ),
      cw,
      cs
    )
  }
}
