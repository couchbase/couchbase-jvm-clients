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
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.options.GetOptions;
import com.couchbase.client.java.options.InsertOptions;
import com.couchbase.client.java.options.RemoveOptions;
import com.couchbase.client.java.options.ReplaceOptions;
import com.couchbase.client.java.options.UpsertOptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * The {@link Collection} provides blocking, synchronous access to all collection APIs.
 *
 * <p>If asynchronous access is needed, we recommend looking at the {@link ReactiveCollection} and
 * if the last drop of performance is needed the {@link AsyncCollection}. This blocking API itself
 * is just a small layer on top of the {@link AsyncCollection} which blocks the current thread
 * until the request completes with a response.</p>
 *
 * @since 3.0.0
 */
public class Collection {

  /**
   * Holds the underlying async collection.
   */
  private final AsyncCollection asyncCollection;

  /**
   * Holds the underlying reactive collection.
   */
  private final ReactiveCollection reactiveCollection;

  public Collection(final String name, final String scope, final Core core,
             final CouchbaseEnvironment environment) {
    asyncCollection = new AsyncCollection(name, scope, core, environment);
    reactiveCollection = new ReactiveCollection(asyncCollection);
  }

  /**
   * Provides access to the underlying {@link AsyncCollection}.
   */
  public AsyncCollection async() {
    return asyncCollection;
  }

  /**
   * Provides access to the underlying {@link ReactiveCollection}.
   */
  public ReactiveCollection reactive() {
    return reactiveCollection;
  }

  /**
   *
   * @param id
   * @return
   */
  public Document<JsonObject> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param options
   * @param <T>
   * @return
   */
  public <T> Document<T> get(final String id, final GetOptions<T> options) {
    return wrapBlockingGet(async().get(id, options));
  }

  /**
   *
   * @param id
   * @param content
   * @param <T>
   * @return
   */
  public <T> MutationResult insert(final String id, final T content) {
    return insert(id, content, InsertOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param content
   * @param options
   * @param <T>
   * @return
   */
  public <T> MutationResult insert(final String id, final T content,
                                   final InsertOptions<T> options) {
    return wrapBlockingGet(async().insert(id, content, options));
  }

  /**
   *
   * @param id
   * @param content
   * @param <T>
   * @return
   */
  public <T> MutationResult upsert(final String id, final T content) {
    return upsert(id, content, UpsertOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param content
   * @param options
   * @param <T>
   * @return
   */
  public <T> MutationResult upsert(final String id, final T content,
                                    final UpsertOptions<T> options) {
    return wrapBlockingGet(async().upsert(id, content, options));
  }

  /**
   *
   * @param id
   * @param content
   * @param <T>
   * @return
   */
  public <T> MutationResult replace(final String id, final T content) {
    return replace(id, content, ReplaceOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param content
   * @param options
   * @param <T>
   * @return
   */
  public <T> MutationResult replace(final String id, final T content,
                                   final ReplaceOptions<T> options) {
    return wrapBlockingGet(async().replace(id, content, options));
  }

  /**
   *
   * @param id
   * @return
   */
  public MutationResult remove(final String id) {
    return remove(id, RemoveOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param options
   * @return
   */
  public MutationResult remove(final String id, final RemoveOptions options) {
    return wrapBlockingGet(async().remove(id, options));
  }

  /**
   * Helper method to wrap an async call into a blocking one and make sure to
   * convert all checked exceptions into their correct runtime counterparts.
   *
   * @param input the future as input.
   * @param <T> the generic type to return.
   * @return blocks and completes on the given future while converting checked exceptions.
   */
  private <T> T wrapBlockingGet(final CompletableFuture<T> input) {
    try {
      return input.get();
    } catch (InterruptedException | ExecutionException e) {
      // todo: figure out if this is the right strategy
      throw new RuntimeException(e);
    }
  }

}
