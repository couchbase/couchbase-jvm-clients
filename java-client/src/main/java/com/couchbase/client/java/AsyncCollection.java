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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.options.GetOptions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * The {@link AsyncCollection} provides basic asynchronous access to all collection APIs.
 *
 * <p>This type of API provides asynchronous support through the concurrency mechanisms
 * that ship with Java 8 and later, notably the async {@link CompletionStage}. It is the
 * async mechanism with the lowest overhead (best performance) but also comes with less
 * bells and whistles as the {@link ReactiveCollection} for example.</p>
 *
 * @since 2.0.0
 */
public class AsyncCollection {

  /**
   * Holds the underlying core which is used to dispatch operations.
   */
  private final Core core;

  /**
   * Holds the core context of the attached core.
   */
  private final CoreContext coreContext;

  /**
   * Holds the environment for this collection.
   */
  private final CouchbaseEnvironment environment;

  /**
   * The name of the collection.
   */
  private final String name;

  /**
   * The scope of the collection.
   */
  private final String scope;

  /**
   * Creates a new {@link AsyncCollection}.
   *
   * @param core the core into which ops are dispatched.
   * @param environment the surrounding environment for config options.
   */
  AsyncCollection(final String name, final String scope, final Core core,
                  final CouchbaseEnvironment environment) {
    this.name = name;
    this.scope = scope;
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  Core core() {
    return core;
  }

  /**
   * Provides access to the underlying {@link CouchbaseEnvironment}.
   */
  CouchbaseEnvironment environment() {
    return environment;
  }

  /**
   * Fetches a {@link Document} from a collection with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} indicating once the document is loaded.
   */
  public CompletableFuture<Document<JsonObject>> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   * Fetches a {@link Document} from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @param <T> the content type of the returned {@link Document}.
   * @return a {@link CompletableFuture} indicating once the document is loaded.
   */
  public <T> CompletableFuture<Document<T>> get(final String id, final GetOptions<T> options) {
    notNullOrEmpty(id, "ID");
    notNull(options, "GetOptions");

    Duration timeout = options.timeout().orElse(environment.kvTimeout());
    GetRequest request = new GetRequest(id, timeout, coreContext);
    return get(id, request, options.decodeInto());
  }

  /**
   * Internal: Take a {@link GetRequest} and dispatch, convert and return the result.
   *
   * @param id the document ID as a string.
   * @param request the request to dispatch and analyze.
   * @param convertInto into which response type it should be converted.
   * @param <T> the generic type of the response document.
   * @return a {@link CompletableFuture} once the document is fetched and decoded.
   */
  @Stability.Internal
  <T> CompletableFuture<Document<T>> get(final String id, final GetRequest request,
                                         final Class<T> convertInto) {
    dispatch(request);
    return request
      .response()
      .thenApply(getResponse -> {
        // todo: implement decoding and response code checking
        return new Document<>(id, null, getResponse.cas());
      });
  }

  /**
   * Helper method to dispatch the given {@link Request} into the core.
   *
   * @param request the request to send/dispatch.
   */
  private void dispatch(final Request<? extends Response> request) {
    core.send(request);
  }

}
