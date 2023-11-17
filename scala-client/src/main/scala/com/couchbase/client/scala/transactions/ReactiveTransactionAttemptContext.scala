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

package com.couchbase.client.scala.transactions;

import com.couchbase.client.core.api.query.{CoreQueryContext, CoreQueryOptions}
import com.couchbase.client.core.cnc.{CbTracing, RequestSpan, TracingIdentifiers}
import com.couchbase.client.core.cnc.TracingIdentifiers.{TRANSACTION_OP_INSERT, TRANSACTION_OP_REMOVE, TRANSACTION_OP_REPLACE}
import com.couchbase.client.core.transaction.CoreTransactionAttemptContext
import com.couchbase.client.core.transaction.support.SpanWrapper
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.transactions.internal.EncodingUtil.encode
import com.couchbase.client.scala.util.FutureConversions
import com.couchbase.client.scala.{ReactiveCollection, ReactiveScope}
import reactor.core.scala.publisher.SMono

import scala.util.{Failure, Success};

/**
  * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well
  * as commit or rollback the transaction.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  */
class ReactiveTransactionAttemptContext private[scala] (
    private[client] val internal: CoreTransactionAttemptContext
) {

  /**
    * Gets a document with the specified <code>id</code> and from the specified Couchbase <code>collection</code>.
    * <p>
    * If the document does not exist it will raise a [[com.couchbase.client.core.error.DocumentNotFoundException]].
    *
    * @param collection the Couchbase collection the document exists on
    * @param id         the document's ID
    * @return a <code>TransactionGetResult</code> containing the document
    */
  def get(collection: ReactiveCollection, id: String): SMono[TransactionGetResult] = {
    FutureConversions
      .javaMonoToScalaMono(internal.get(collection.collectionIdentifier, id))
      .map(TransactionGetResult)
  }

  /**
    * Inserts a new document into the specified Couchbase <code>collection</code>.
    *
    * @param collection the Couchbase collection in which to insert the doc
    * @param id         the document's unique ID
    * @param content       $SupportedTypes
    * @return the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
    */
  def insert[T](collection: ReactiveCollection, id: String, content: T)(
      implicit serializer: JsonSerializer[T]
  ): SMono[TransactionGetResult] = {
    val span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_INSERT, internal.span())
    span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_INSERT)
    encode(content, span, serializer, internal.core.context) match {
      case Failure(exception) => SMono.raiseError(exception)
      case Success(encoded) =>
        FutureConversions
          .javaMonoToScalaMono(
            internal.insert(collection.collectionIdentifier, id, encoded, new SpanWrapper(span))
          )
          .map(TransactionGetResult)
          .doOnError(_ => span.status(RequestSpan.StatusCode.ERROR))
          .doOnTerminate(() => span.end())
    }
  }

  /**
    * Mutates the specified <code>doc</code> with new content.
    *
    * @param doc     the doc to be mutated
    * @param content       $SupportedTypes
    * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
    * object is modified.
    */
  def replace[T](doc: TransactionGetResult, content: T)(
      implicit serializer: JsonSerializer[T]
  ): SMono[TransactionGetResult] = {
    val span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REPLACE, internal.span())
    span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_REPLACE)
    encode(content, span, serializer, internal.core.context) match {
      case Failure(exception) => SMono.raiseError(exception)
      case Success(encoded) =>
        FutureConversions
          .javaMonoToScalaMono(internal.replace(doc.internal, encoded, new SpanWrapper(span)))
          .map(TransactionGetResult)
          .doOnError(_ => span.status(RequestSpan.StatusCode.ERROR))
          .doOnTerminate(() => span.end())
    }
  }

  /**
    * Removes the specified <code>doc</code>.
    * <p>
    * @param doc - the doc to be removed
    */
  def remove(doc: TransactionGetResult): SMono[Unit] = {
    val span = CbTracing.newSpan(internal.core().context(), TRANSACTION_OP_REMOVE, internal.span())
    span.lowCardinalityAttribute(TracingIdentifiers.ATTR_OPERATION, TRANSACTION_OP_REMOVE)
    FutureConversions
      .javaMonoToScalaMono(internal.remove(doc.internal, new SpanWrapper(span)))
      .doOnError(_ => span.status(RequestSpan.StatusCode.ERROR))
      .doOnTerminate(() => span.end())
      .`then`()
  }

  /**
    * Runs a N1QL query and returns the result.
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
  ): SMono[TransactionQueryResult] = {
    query(null, statement, null)
  }

  /**
    * Runs a N1QL query and returns the result.
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
  ): SMono[TransactionQueryResult] = {
    query(null, statement, options)
  }

  /**
    * Runs a N1QL query and returns the result.
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
      scope: ReactiveScope,
      statement: String
  ): SMono[TransactionQueryResult] = {
    query(scope, statement, null)
  }

  /**
    * Runs a N1QL query and returns the result.
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
      scope: ReactiveScope,
      statement: String,
      options: TransactionQueryOptions
  ): SMono[TransactionQueryResult] = {
    val opts: CoreQueryOptions = Option(options).map(v => v.toCore).orNull
    FutureConversions
      .javaMonoToScalaMono(
        internal.queryBlocking(
          statement,
          if (scope == null) null else CoreQueryContext.of(scope.bucketName, scope.name),
          opts,
          false
        )
      )
      .publishOn(
        internal.core().context().environment().transactionsSchedulers().schedulerBlocking()
      )
      .map(TransactionQueryResult)
  }
}
