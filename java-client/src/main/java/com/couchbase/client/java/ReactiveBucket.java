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
import com.couchbase.client.java.env.ClusterEnvironment;

public class ReactiveBucket {

  private final AsyncBucket asyncBucket;
  private final Core core;
  private final ClusterEnvironment environment;

  ReactiveBucket(AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.core = asyncBucket.core();
    this.environment = asyncBucket.environment();
  }

  public AsyncBucket async() {
    return asyncBucket;
  }

  public ReactiveCollection defaultCollection() {
    return collection(null, null);
  }

  public ReactiveCollection collection(final String name) {
    return collection(name, null);
  }

  public ReactiveCollection collection(final String name, final String scope) {
    return new ReactiveCollection(new AsyncCollection(name, scope, asyncBucket.name(), core, environment), asyncBucket.name());
  }

}
