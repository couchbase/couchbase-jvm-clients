/*
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
package com.couchbase.client.scala.transactions

import com.couchbase.client.core.io.CollectionIdentifier
import scala.compat.java8.OptionConverters._

/** A keyspace represents a triple of bucket, scope and collection.
  *
  * @param scope if empty, it specifies the default scope of the bucket.
  * @param collection if empty, it specifies the default collection of the scope.
  */
case class TransactionKeyspace(
    bucket: String,
    scope: Option[String] = None,
    collection: Option[String] = None
) {
  private[scala] def toCollectionIdentifier =
    new CollectionIdentifier(bucket, scope.asJava, collection.asJava)
}
