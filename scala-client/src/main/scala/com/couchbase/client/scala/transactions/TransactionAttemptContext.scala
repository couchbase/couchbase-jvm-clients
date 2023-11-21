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

import com.couchbase.client.core.error.CouchbaseException
import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.{Collection, Scope}

import scala.util.Try

/**
  * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents.
  * <p>
  * These methods are blocking/synchronous.  See [[ReactiveTransactionAttemptContext]] for the asynchronous version.
  *
  * @define SupportedTypes this can be of any type for which an implicit
  *                        `com.couchbase.client.scala.codec.Conversions.JsonSerializer` can be found: a list
  *                        of types that are supported 'out of the box' is available at
  *                        [[https://docs.couchbase.com/scala-sdk/current/howtos/json.html these JSON docs]]
  */
class TransactionAttemptContext private[scala] (
    private[client] val internal: ReactiveTransactionAttemptContext
) {

  /**
    * Gets a document from the specified Couchbase <code>collection</code> matching the specified <code>id</code>.  If
    * the document is not found, a [[com.couchbase.client.core.error.DocumentNotFoundException]] is raised.
    *
    * @param collection the Couchbase collection the document exists on
    * @param id     the document's ID
    * @return a [[TransactionGetResult]] containing the document
    */
  def get(collection: Collection, id: String): Try[TransactionGetResult] = {
    Try(internal.get(collection.reactive, id).block())
  }

  /**
    * Mutates the specified <code>doc</code> with new content.
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
    * @return the doc, updated with its new CAS value.  For performance a copy is not created and the original doc
    * object is modified.
    */
  def replace[T](doc: TransactionGetResult, content: T)(
      implicit serializer: JsonSerializer[T]
  ): Try[TransactionGetResult] = {
    Try(internal.replace(doc, content).block())
  }

  /**
    * Inserts a new document into the specified Couchbase <code>collection</code>.
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
    * @the doc, updated with its new CAS value and ID, and converted to a <code>TransactionGetResult</code>
    */
  def insert[T](collection: Collection, id: String, content: T)(
      implicit serializer: JsonSerializer[T]
  ): Try[TransactionGetResult] = {
    Try(internal.insert(collection.reactive, id, content).block())
  }

  /**
    * Removes the specified <code>doc</code>.
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
    Try(internal.remove(doc).block())
  }

  /**
    * Runs a N1QL query and returns the result.
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

  /**
    * Runs a N1QL query and returns the result.
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

  /**
    * Runs a N1QL query and returns the result.
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

  /**
    * Runs a N1QL query and returns the result.
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
    Try(internal.query(if (scope != null) scope.reactive else null, statement, options).block())
  }
}
