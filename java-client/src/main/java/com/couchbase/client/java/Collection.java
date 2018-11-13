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

  public Collection(Core core, CouchbaseEnvironment environment) {
    asyncCollection = new AsyncCollection(core, environment);
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
  public Document<JsonObject> get(String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   *
   * @param id
   * @param options
   * @param <T>
   * @return
   */
  public <T> Document<T> get(String id, GetOptions<T> options) {
    return wrapBlockingGet(asyncCollection.get(id, options));
  }

  private <T> T wrapBlockingGet(final CompletableFuture<T> input) {
    try {
      return input.get();
    } catch (InterruptedException | ExecutionException e) {
      // todo: figure out if this is the right strategy
      throw new RuntimeException(e);
    }
  }

}
