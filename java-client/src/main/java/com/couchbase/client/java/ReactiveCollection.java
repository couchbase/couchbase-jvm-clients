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

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.options.GetOptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

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

  ReactiveCollection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
    this.coreContext = asyncCollection.core().context();
    this.environment = asyncCollection.environment();
  }

  /**
   * Provides access to the underlying {@link AsyncCollection}.
   */
  public AsyncCollection async() {
    return asyncCollection;
  }

  /**
   *
   * @param id
   * @return
   */
  public Mono<GetResult> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param options
   * @return
   */
  public Mono<GetResult> get(final String id, final GetOptions options) {
    notNullOrEmpty(id, "ID");
    notNull(options, "GetOptions");

    return Mono.defer((Supplier<Mono<GetResult>>) () -> {
      Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
      GetRequest request = new GetRequest(id, timeout, coreContext);

      CompletableFuture<Optional<GetResult>> response = async().get(request);
      return Reactor
        .wrap(request, response, true)
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
    });
  }

}
