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
package com.couchbase.client.scala.transactions.config

import com.couchbase.client.scala.query.QueryScanConsistency;

/**
  * Allows setting a default query configuration for all transactions.
  */
case class TransactionsQueryConfig private[scala] (
    private[scala] val scanConsistency: Option[QueryScanConsistency] = None
) {

  /**
    * Customizes the default consistency guarantees for all queries inside this transaction.
    * <p>
    * Tuning the scan consistency allows to trade data "freshness" for latency and vice versa. By default
    * [[QueryScanConsistency.RequestPlus]] is used for any queries inside a transaction, which means that the
    * indexer will wait until any indexes used are consistent with all mutations at the time of the query.
    * If this level of consistency is not required, use [[QueryScanConsistency.NotBounded]] which will execute
    * the query immediately with whatever data are in the index.
    * <p>
    * @param scanConsistency the index scan consistency to be used.
    * @return a builder with the value set
    */
  def scanConsistency(scanConsistency: QueryScanConsistency): TransactionsQueryConfig = {
    copy(scanConsistency = Some(scanConsistency))
  }
}
