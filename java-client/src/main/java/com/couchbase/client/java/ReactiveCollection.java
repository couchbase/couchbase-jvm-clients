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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.Document;
import com.couchbase.client.java.kv.GetOptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * The {@link ReactiveCollection} provides sophisticated asynchronous access to all collection APIs.
 *
 * <p>This API provides more sophisticated async controls over the {@link AsyncCollection}, but
 * it also comes with a little more overhead. For most use cases we recommend using this API
 * over the other one, unless you really need that last drop of performance and can live with the
 * significantly reduced functionality (in terms of the richness of operators). For example, this
 * {@link ReactiveCollection} is built on top of the {@link AsyncCollection}.</p>
 *
 * @since 3.0.0
 */
public class ReactiveCollection {

  /**
   * Holds the underlying async collection.
   */
  private final AsyncCollection asyncCollection;

  /**
   * Holds the core context of the attached core.
   */
  private final CoreContext coreContext;

  /**
   * Holds the environment for this collection.
   */
  private final CouchbaseEnvironment environment;

  /**
   * Holds a direct reference to the core.
   */
  private final Core core;

  ReactiveCollection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
    this.coreContext = asyncCollection.core().context();
    this.environment = asyncCollection.environment();
    this.core = asyncCollection.core();
  }

  /**
   * Provides access to the underlying {@link AsyncCollection}.
   *
   * @return returns the underlying {@link AsyncCollection}.
   */
  public AsyncCollection async() {
    return asyncCollection;
  }

  /**
   * Fetches a Document (or a fragment of it) from a collection with default options.
   *
   * <p>If the document has not been found, this {@link Mono} will be empty.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<Document> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   * Fetches a Document (or a fragment of it) from a collection with custom options.
   *
   * <p>If the document has not been found, this {@link Mono} will be empty.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<Document> get(final String id, final GetOptions options) {
    notNullOrEmpty(id, "ID");
    notNull(options, "GetOptions");

    return Mono.defer(() -> {
      Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
      GetRequest request = new GetRequest(id, timeout, coreContext);
      return Reactor
        .wrap(request, GetAccessor.get(core, request), true)
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
    });
  }

}
