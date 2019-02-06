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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import com.couchbase.client.core.msg.kv.PrependRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.kv.AppendAccessor;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterAccessor;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependAccessor;
import com.couchbase.client.java.kv.PrependOptions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

public class AsyncBinaryCollection {

  private final Core core;
  private final CoreContext coreContext;
  private final CoreEnvironment environment;
  private final String bucket;
  private final byte[] collectionId;

  AsyncBinaryCollection(final Core core, final CoreEnvironment environment, final String bucket,
                        final byte[] collectionId) {
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
    this.bucket = bucket;
    this.collectionId = collectionId;
  }

  public CompletableFuture<MutationResult> append(final String id, final byte[] content) {
    return append(id, content, AppendOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> append(final String id, final byte[] content,
                                                  final AppendOptions options) {
    return AppendAccessor.append(core, appendRequest(id, content, options));
  }

  AppendRequest appendRequest(final String id, final byte[] content, final AppendOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "AppendOptions");

    Duration timeout = options.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment.retryStrategy());
    return new AppendRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId, content,
      options.cas(), options.durabilityLevel());
  }

  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content) {
    return prepend(id, content, PrependOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content,
                                                   final PrependOptions options) {
    return PrependAccessor.prepend(core, prependRequest(id, content, options));
  }

  PrependRequest prependRequest(final String id, final byte[] content, final PrependOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "PrependOptions");

    Duration timeout = options.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment.retryStrategy());
    return new PrependRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId, content,
      options.cas(), options.durabilityLevel());
  }

  public CompletableFuture<CounterResult> increment(final String id) {
    return increment(id, IncrementOptions.DEFAULT);
  }

  public CompletableFuture<CounterResult> increment(final String id, final IncrementOptions options) {
    return CounterAccessor.increment(core, incrementRequest(id, options));
  }

  IncrementRequest incrementRequest(final String id, final IncrementOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "IncrementOptions");

    Duration timeout = options.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment.retryStrategy());
    return new IncrementRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId,
      options.delta(), options.initial(), options.expiry(), options.durabilityLevel());
  }

  public CompletableFuture<CounterResult> decrement(final String id) {
    return decrement(id, DecrementOptions.DEFAULT);
  }

  public CompletableFuture<CounterResult> decrement(final String id, final DecrementOptions options) {
    return CounterAccessor.decrement(core, decrementRequest(id, options));
  }

  DecrementRequest decrementRequest(final String id, final DecrementOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "DecrementOptions");

    Duration timeout = options.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment.retryStrategy());
    return new DecrementRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId,
      options.delta(), options.initial(), options.expiry(), options.durabilityLevel());
  }
}
