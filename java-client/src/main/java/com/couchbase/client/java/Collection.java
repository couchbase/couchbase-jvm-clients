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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class Collection {

  private final AsyncCollection asyncCollection;
  private final ReactiveCollection reactiveCollection;

  Collection(final String name, final String scope, final Core core, final CouchbaseEnvironment environment) {
    asyncCollection = new AsyncCollection(name, scope, core, environment);
    reactiveCollection = new ReactiveCollection(asyncCollection);
  }

  /**
   *
   * @return
   */
  public AsyncCollection async() {
    return asyncCollection;
  }

  /**
   *
   * @return
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
