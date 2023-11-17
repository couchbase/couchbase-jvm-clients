/**
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.scala.transactions;

import com.couchbase.client.core.api.query.CoreQueryResult
import com.couchbase.client.scala.codec.JsonDeserializer
import com.couchbase.client.scala.query.QueryMetaData
import com.couchbase.client.scala.util.{CoreCommonConverters, RowTraversalUtil}

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
  * The result of a N1QL query executed within a transaction, including rows and associated metadata.
  * <p>
  * Queries executed inside a transaction are always blocking/non-streaming, to allow essential error handling logic to
  * reliably take place.
  */
case class TransactionQueryResult private[scala] (private val internal: CoreQueryResult) {

  def rowsAs[T](implicit deserializer: JsonDeserializer[T]): Try[collection.Seq[T]] = {
    RowTraversalUtil.traverse(
      internal
        .rows()
        .iterator
        .asScala
        .map(row => {
          deserializer.deserialize(row.data())
        })
    )
  }

  /**
    * Returns the {@link QueryMetaData} giving access to the additional metadata associated with this query.
    */
  def metaData(): QueryMetaData = {
    CoreCommonConverters.convert(internal.metaData())
  }
}
