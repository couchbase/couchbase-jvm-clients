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

import com.couchbase.client.java.view.SpatialViewOptions;
import com.couchbase.client.java.view.ViewOptions;
import com.couchbase.client.java.view.ViewResult;

import static com.couchbase.client.java.AsyncBucket.DEFAULT_SCOPE;
import static com.couchbase.client.java.AsyncUtils.block;

public class Bucket {

  private final AsyncBucket asyncBucket;
  private final ReactiveBucket reactiveBucket;

  Bucket(AsyncBucket asyncBucket) {
    this.asyncBucket = asyncBucket;
    this.reactiveBucket = new ReactiveBucket(asyncBucket);
  }

  public AsyncBucket async() {
    return asyncBucket;
  }

  public ReactiveBucket reactive() {
    return reactiveBucket;
  }

  public Scope scope(final String name) {
    return block(asyncBucket.scope(name)
      .thenApply(Scope::new)
    );
  }

  public Collection defaultCollection() {
    return scope(DEFAULT_SCOPE).defaultCollection();
  }

  public Collection collection(final String name) {
    return scope(DEFAULT_SCOPE).collection(name);
  }

  public ViewResult viewQuery(final String designDoc, final String viewName) {
    return block(asyncBucket.viewQuery(designDoc, viewName)
      .thenApply(avr -> new ViewResult()));
  }

  public ViewResult viewQuery(final String designDoc, final String viewName,
                              final ViewOptions options) {
    return block(asyncBucket.viewQuery(designDoc, viewName, options)
      .thenApply(avr -> new ViewResult()));

  }

  public ViewResult spatialViewQuery(final String designDoc, final String viewName) {
    return block(asyncBucket.spatialViewQuery(designDoc, viewName)
      .thenApply(avr -> new ViewResult()));
  }

  public ViewResult spatialViewQuery(final String designDoc, final String viewName,
                                     final SpatialViewOptions options) {
    return block(asyncBucket.spatialViewQuery(designDoc, viewName, options)
      .thenApply(avr -> new ViewResult()));

  }

}
