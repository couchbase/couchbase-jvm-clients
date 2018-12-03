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

public class Bucket {

  private final AsyncBucket asyncBucket;
  private final ReactiveBucket reactiveBucket;
  private final Core core;
  private final ClusterEnvironment environment;

  Bucket(AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.reactiveBucket = new ReactiveBucket(asyncBucket);
    this.core = asyncBucket.core();
    this.environment = asyncBucket.environment();
  }

  public AsyncBucket async() {
    return asyncBucket;
  }

  public ReactiveBucket reactive() {
    return reactiveBucket;
  }

  public Collection defaultCollection() {
    return collection(null, null);
  }

  public Collection collection(final String name) {
    return collection(name, null);
  }

  public Collection collection(final String name, final String scope) {
    return new Collection(new AsyncCollection(name, scope, core, environment));
  }

}
