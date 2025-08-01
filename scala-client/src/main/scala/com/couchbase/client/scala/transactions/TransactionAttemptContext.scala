/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
package com.couchbase.client.scala.transactions

import com.couchbase.client.core.cnc.{CbTracing, RequestSpan, TracingIdentifiers}
import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext
import com.couchbase.client.core.transaction.log.CoreTransactionLogger
import com.couchbase.client.core.transaction.support.SpanWrapper
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.transactions.getmulti.{
  TransactionGetMultiSpec,
  TransactionGetMultiOptions,
  TransactionGetMultiResult,
  TransactionGetMultiReplicasFromPreferredServerGroupSpec,
  TransactionGetMultiReplicasFromPreferredServerGroupOptions,
  TransactionGetMultiReplicasFromPreferredServerGroupResult,
  TransactionGetMultiUtil
}
import com.couchbase.client.scala.transactions.config.{
  TransactionGetOptions,
  TransactionGetReplicaFromPreferredServerGroupOptions,
  TransactionInsertOptions,
  TransactionReplaceOptions
}
import com.couchbase.client.scala.transactions.internal.EncodingUtil.encode
import com.couchbase.client.scala.util.{AsyncUtils, FutureConversions}
import com.couchbase.client.scala.{Collection, Scope}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/** Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents.
  * <p>
  * These methods are blocking/synchronous.  See [[ReactiveTransactionAttemptContext]] for the asynchronous version.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  */
class TransactionAttemptContext private[scala] (
    private[client] val internal: AsyncTransactionAttemptContext
) {

  /** Gets a document from the specified Couchbase <code>collection</code> matching the specified <code>id</code>.  If
    * the document is not found, a [[com.couchbase.client.core.error.DocumentNotFoundException]] is raised.
    *
    * @param collection the Couchbase collection the document exists on
    * @param id     the document's ID
    * @param options    options controlling the operation
    * @return a [[TransactionGetResult]] containing the document
    */
  def get(
      collection: Collection,
      id: String,
      options: TransactionGetOptions = TransactionGetOptions.Default
  ): Try[TransactionGetResult] = {
    AsyncUtils.block(internal.get(collection.async, id, options))
  }

  /** Gets a document from the specified Couchbase <code>collection</code> matching the specified <code>id</code>.
    * <p>
    * It will be fetched only from document copies that on nodes in the preferred server group, which can
    * be configured with [[com.couchbase.client.scala.env.ClusterEnvironment.Builder.preferredServerGroup]].
    * <p>
    * If no replica can be retrieved, which can include for reasons such as this preferredServerGroup not being set,
    * and misconfigured server groups, then [[com.couchbase.client.core.error.DocumentUnretrievableException]]
    * can be raised.  It is strongly recommended that this method always be used with a fallback strategy to use
    * ctx.get() on failure.
    *
    * @param collection the Couchbase collection the document exists on
    * @param id         the document's ID
    * @param options    options controlling the operation
    * @return a <code>TransactionGetResult</code> containing the document
    */
  def getReplicaFromPreferredServerGroup(
      collection: Collection,
      id: String,
      options: TransactionGetReplicaFromPreferredServerGroupOptions =
        TransactionGetReplicaFromPreferredServerGroupOptions.Default
  ): Try[TransactionGetResult] = {
    AsyncUtils.block(internal.getReplicaFromPreferredServerGroup(collection.async, id, options))
  }

  /** Fetches multiple documents in a single operation.
    *
    * In addition, it will heuristically aim to detect read skew anomalies, and avoid them if possible.  Read skew detection and avoidance is not guaranteed.
    *
    * @param specs the documents to fetch.
    * @return a result containing the fetched documents.
    */
  def getMulti(
      specs: Seq[TransactionGetMultiSpec],
      options: TransactionGetMultiOptions = TransactionGetMultiOptions.Default
  ): Try[TransactionGetMultiResult] = {
    AsyncUtils.block(internal.getMulti(specs, options))
  }

  /** Similar to [[getMulti]], but fetches the documents from replicas in the preferred server group.
    *
    * Note that the nature of replicas is that they are eventually consistent with the active, and so the effectiveness of read skew detection may be impacted.
    */
  def getMultiReplicasFromPreferredServerGroup(
      specs: Seq[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
      options: TransactionGetMultiReplicasFromPreferredServerGroupOptions =
        TransactionGetMultiReplicasFromPreferredServerGroupOptions.Default
  ): Try[TransactionGetMultiReplicasFromPreferredServerGroupResult] = {
    AsyncUtils.block(internal.getMultiReplicasFromPreferredServerGroup(specs, options))
  }

  /** Mutates the specified <code>doc</code> with new content.
    * <p>
    * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase
    * component will see the document's current value, rather than this staged or 'dirty' data.  If the attempt is
    * rolled back, the staged mutation will be removed.
    * <p>
    * This staged data effectively locks the document from other transactional writes until the attempt completes
    * (commits or rolls back).
    *
    * @param doc     the doc to be updated
    * @param content       $SupportedTypes
    * @param options    options controlling the operation
    * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
    * object is modified.
    */
  def replace[T](
      doc: TransactionGetResult,
      content: T,
      options: TransactionReplaceOptions = TransactionReplaceOptions.Default
  )(
      implicit serializer: JsonSerializer[T]
  ): Try[TransactionGetResult] = {
    AsyncUtils.block(internal.replace(doc, content, options))
  }

  /** Inserts a new document into the specified Couchbase <code>collection</code>.
    * <p>
    * The insert is staged until the transaction is committed.  No other actor will be able to see this inserted
    * document until that point.
    * <p>
    * This staged data effectively locks the document from other transactional writes until the attempt completes
    * (commits or rolls back).
    *
    * @param collection the Couchbase collection in which to insert the doc
    * @param id         the document's unique ID
    * @param content       $SupportedTypes
    * @param options    options controlling the operation
    * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
    */
  def insert[T](
      collection: Collection,
      id: String,
      content: T,
      options: TransactionInsertOptions = TransactionInsertOptions.Default
  )(
      implicit serializer: JsonSerializer[T]
  ): Try[TransactionGetResult] = {
    AsyncUtils.block(internal.insert(collection.async, id, content, options))
  }

  /** Removes the specified <code>doc</code>.
    * <p>
    * The remove is staged until the transaction is committed.  That is, the document will
    * continue to exist, and the rest of the Couchbase platform will continue to see it.
    * <p>
    * This staged data effectively locks the document from other transactional writes until the attempt completes
    * (commits or rolls back).
    *
    * @param doc the doc to be removed
    */
  def remove(doc: TransactionGetResult): Try[Unit] = {
    AsyncUtils.block(internal.remove(doc))
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * Raises [[CouchbaseException]] or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will generally
    * cause the attempt to fail.
    */
  def query(statement: String): Try[TransactionQueryResult] = {
    query(null, statement, null)
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    *
    * Raises [[CouchbaseException]] or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will generally
    * cause the attempt to fail.
    */
  def query(statement: String, options: TransactionQueryOptions): Try[TransactionQueryResult] = {
    query(null, statement, options)
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
    * query statement, without needing to specify the full bucket.scope.collection syntax.
    * <p>
    * Raises [[CouchbaseException]] or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will generally
    * cause the attempt to fail.
    */
  def query(scope: Scope, statement: String): Try[TransactionQueryResult] = {
    query(scope, statement, null)
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
    * query statement, without needing to specify the full bucket.scope.collection syntax.
    * <p>
    *
    * Raises [[CouchbaseException]] or an error derived from it on failure.  The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will generally
    * cause the attempt to fail.
    */
  def query(
      scope: Scope,
      statement: String,
      options: TransactionQueryOptions
  ): Try[TransactionQueryResult] = {
    AsyncUtils.block(internal.query(if (scope == null) null else scope.async, statement, options))
  }

  private[client] def logger(): CoreTransactionLogger = {
    internal.internal.logger()
  }
}
