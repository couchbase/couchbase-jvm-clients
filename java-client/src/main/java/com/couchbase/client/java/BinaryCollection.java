/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.RequestTimeoutException;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependOptions;

import static com.couchbase.client.java.AsyncUtils.block;

public class BinaryCollection {

  private final AsyncBinaryCollection async;

  BinaryCollection(AsyncBinaryCollection asyncBinaryCollection) {
    this.async = asyncBinaryCollection;
  }

  /**
   * Appends binary content to the document.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult append(final String id, final byte[] content) {
    return block(async.append(id, content));
  }

  /**
   * Appends binary content to the document with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @param options custom options to customize the append behavior.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult append(final String id, final byte[] content, final AppendOptions options) {
    return block(async.append(id, content, options));
  }

  /**
   * Prepends binary content to the document.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult prepend(final String id, final byte[] content) {
    return block(async.prepend(id, content));
  }

  /**
   * Prepends binary content to the document with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @param options custom options to customize the prepend behavior.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult prepend(final String id, final byte[] content, final PrependOptions options) {
    return block(async.prepend(id, content, options));
  }

  public CounterResult increment(final String id) {
    return block(async.increment(id));
  }

  public CounterResult increment(final String id, final IncrementOptions options) {
    return block(async.increment(id, options));
  }

  public CounterResult decrement(final String id) {
    return block(async.decrement(id));
  }

  public CounterResult decrement(final String id, final DecrementOptions options) {
    return block(async.decrement(id, options));
  }

}
