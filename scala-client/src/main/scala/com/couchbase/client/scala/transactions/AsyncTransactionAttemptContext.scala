/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.scala.transactions

import com.couchbase.client.core.api.query.{CoreQueryContext, CoreQueryOptions}
import com.couchbase.client.core.cnc.TracingIdentifiers.{
  TRANSACTION_OP_INSERT,
  TRANSACTION_OP_REMOVE,
  TRANSACTION_OP_REPLACE
}
import com.couchbase.client.core.cnc.{CbTracing, RequestSpan, TracingIdentifiers}
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext
import com.couchbase.client.core.transaction.support.SpanWrapper
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.env.ClusterEnvironment
import com.couchbase.client.scala.transactions.config.{
  TransactionGetOptions,
  TransactionGetReplicaFromPreferredServerGroupOptions,
  TransactionInsertOptions,
  TransactionReplaceOptions
}
import com.couchbase.client.scala.transactions.getmulti.{
  TransactionGetMultiOptions,
  TransactionGetMultiReplicasFromPreferredServerGroupOptions,
  TransactionGetMultiReplicasFromPreferredServerGroupResult,
  TransactionGetMultiReplicasFromPreferredServerGroupSpec,
  TransactionGetMultiResult,
  TransactionGetMultiSpec,
  TransactionGetMultiUtil
}
import com.couchbase.client.scala.transactions.internal.EncodingUtil.encode
import com.couchbase.client.scala.util.FutureConversions
import com.couchbase.client.scala.{AsyncCollection, AsyncScope}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success};

/** Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  */
class AsyncTransactionAttemptContext private[scala] (
    private[client] val internal: CoreTransactionAttemptContext,
    private val environment: ClusterEnvironment
) {
  implicit val executionContext: ExecutionContext = environment.ec

  /** Gets a document with the specified <code>id</code> and from the specified Couchbase <code>collection</code>.
    * <p>
    * If the document does not exist it will raise a [[com.couchbase.client.core.error.DocumentNotFoundException]].
    *
    * @param collection the Couchbase collection the document exists on
    * @param id         the document's ID
    * @param options    options controlling the operation
    * @return a <code>TransactionGetResult</code> containing the document
    */
  def get(
      collection: AsyncCollection,
      id: String,
      options: TransactionGetOptions = TransactionGetOptions.Default
  ): Future[TransactionGetResult] = {
    FutureConversions
      .javaMonoToScalaFuture(internal.get(collection.collectionIdentifier, id))
      .map(result => TransactionGetResult(result, options.transcoder))
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
      collection: AsyncCollection,
      id: String,
      options: TransactionGetReplicaFromPreferredServerGroupOptions =
        TransactionGetReplicaFromPreferredServerGroupOptions.Default
  ): Future[TransactionGetResult] =
    FutureConversions
      .javaMonoToScalaFuture(
        internal.getReplicaFromPreferredServerGroup(collection.collectionIdentifier, id)
      )
      .map(result => TransactionGetResult(result, options.transcoder))

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
  ): Future[TransactionGetMultiResult] = {
    val coreSpecs = TransactionGetMultiUtil.convert(specs)
    FutureConversions
      .javaMonoToScalaFuture(
        internal.getMultiAlgo(coreSpecs, options.toCore, false)
      )
      .map(res => TransactionGetMultiUtil.convert(res, specs))
  }

  /** Similar to [[getMulti]], but fetches the documents from replicas in the preferred server group.
    *
    * Note that the nature of replicas is that they are eventually consistent with the active, and so the effectiveness of read skew detection may be impacted.
    */
  def getMultiReplicasFromPreferredServerGroup(
      specs: Seq[TransactionGetMultiReplicasFromPreferredServerGroupSpec],
      options: TransactionGetMultiReplicasFromPreferredServerGroupOptions =
        TransactionGetMultiReplicasFromPreferredServerGroupOptions.Default
  ): Future[TransactionGetMultiReplicasFromPreferredServerGroupResult] = {
    val coreSpecs = TransactionGetMultiUtil.convertReplica(specs)
    FutureConversions
      .javaMonoToScalaFuture(
        internal.getMultiAlgo(coreSpecs, options.toCore, true)
      )
      .map(res => TransactionGetMultiUtil.convertReplica(res, specs))
  }

  /** Inserts a new document into the specified Couchbase <code>collection</code>.
    *
    * @param collection the Couchbase collection in which to insert the doc
    * @param id         the document's unique ID
    * @param content    $SupportedTypes
    * @param options    options controlling the operation
    * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
    */
  def insert[T](
      collection: AsyncCollection,
      id: String,
      content: T,
      options: TransactionInsertOptions = TransactionInsertOptions.Default
  )(
      implicit serializer: JsonSerializer[T]
  ): Future[TransactionGetResult] = {
    val span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_INSERT, internal.span())
    encode(content, span, serializer, options.transcoder, internal.core.context) match {
      case Failure(exception) => Future.failed(exception)
      case Success(encoded)   =>
        closeSpan(
          span,
          FutureConversions
            .javaMonoToScalaFuture(
              internal.insert(
                collection.collectionIdentifier,
                id,
                encoded.encoded,
                encoded.flags,
                null,
                new SpanWrapper(span)
              )
            )
            .map(result => TransactionGetResult(result, options.transcoder))
        )
    }
  }

  /** Mutates the specified <code>doc</code> with new content.
    *
    * @param doc     the doc to be mutated
    * @param content $SupportedTypes
    * @param options options controlling the operation
    * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
    * object is modified.
    */
  def replace[T](
      doc: TransactionGetResult,
      content: T,
      options: TransactionReplaceOptions = TransactionReplaceOptions.Default
  )(
      implicit serializer: JsonSerializer[T]
  ): Future[TransactionGetResult] = {
    val span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REPLACE, internal.span())
    encode(content, span, serializer, options.transcoder, internal.core.context) match {
      case Failure(exception) => Future.failed(exception)
      case Success(encoded)   =>
        closeSpan(
          span,
          FutureConversions
            .javaMonoToScalaFuture(
              internal
                .replace(doc.internal, encoded.encoded, encoded.flags, null, new SpanWrapper(span))
            )
            .map(result => TransactionGetResult(result, options.transcoder))
        )
    }
  }

  private def closeSpan[T](
      span: RequestSpan,
      future: Future[TransactionGetResult]
  ): Future[TransactionGetResult] = {
    future.onComplete {
      case Failure(_) =>
        span.status(RequestSpan.StatusCode.ERROR)
        span.end()
      case Success(_) =>
        span.end()
    }
    future
  }

  /** Removes the specified <code>doc</code>.
    * <p>
    *
    * @param doc - the doc to be removed
    */
  def remove(doc: TransactionGetResult): Future[Unit] = {
    val span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REMOVE, internal.span())
    val out = FutureConversions
      .javaMonoToScalaFuture(internal.remove(doc.internal, new SpanWrapper(span)))
      .map(_ => ())

    out.onComplete {
      case Failure(_) =>
        span.status(RequestSpan.StatusCode.ERROR)
        span.end()
      case Success(_) =>
        span.end()
    }

    out
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * Raises [[com.couchbase.client.core.error.CouchbaseException]] or an error derived from it on failure.
    * The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
    * cause the attempt to fail.
    */
  def query(
      statement: String
  ): Future[TransactionQueryResult] = {
    query(null, statement, null)
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * Raises [[com.couchbase.client.core.error.CouchbaseException]] or an error derived from it on failure.
    * The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
    * cause the attempt to fail.
    */
  def query(
      statement: String,
      options: TransactionQueryOptions
  ): Future[TransactionQueryResult] = {
    query(null, statement, options)
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
    * query statement, without needing to specify the full bucket.scope.collection syntax.
    * <p>
    * Raises [[com.couchbase.client.core.error.CouchbaseException]] or an error derived from it on failure.
    * The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
    * cause the attempt to fail.
    */
  def query(
      scope: AsyncScope,
      statement: String
  ): Future[TransactionQueryResult] = {
    query(scope, statement, null)
  }

  /** Runs a N1QL query and returns the result.
    * <p>
    * All rows are buffered in-memory.
    * <p>
    * This overload performs a 'scope-level query': that is, one in which a collection may be referenced by name in the
    * query statement, without needing to specify the full bucket.scope.collection syntax.
    * <p>
    * Raises [[com.couchbase.client.core.error.CouchbaseException]] or an error derived from it on failure.
    * The application can choose to catch and ignore this error, and the
    * transaction attempt is allowed to continue.  This differs from Key-Value operations, whose failure will
    * cause the attempt to fail.
    */
  def query(
      scope: AsyncScope,
      statement: String,
      options: TransactionQueryOptions
  ): Future[TransactionQueryResult] = {
    val opts: CoreQueryOptions = Option(options).map(v => v.toCore).orNull
    FutureConversions
      .javaMonoToScalaFuture(
        internal.queryBlocking(
          statement,
          if (scope == null) null else CoreQueryContext.of(scope.bucketName, scope.name),
          opts,
          false
        )
      )
      .map(TransactionQueryResult.apply)
  }
}
