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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.error.ReducedKeyValueErrorContext;
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import com.couchbase.client.core.msg.kv.PrependRequest;
import com.couchbase.client.java.kv.AppendAccessor;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterAccessor;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependAccessor;
import com.couchbase.client.java.kv.PrependOptions;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.kv.AppendOptions.appendOptions;
import static com.couchbase.client.java.kv.DecrementOptions.decrementOptions;
import static com.couchbase.client.java.kv.IncrementOptions.incrementOptions;
import static com.couchbase.client.java.kv.PrependOptions.prependOptions;

public class ReactiveBinaryCollection {

  static final PrependOptions DEFAULT_PREPEND_OPTIONS = prependOptions();
  static final AppendOptions DEFAULT_APPEND_OPTIONS = appendOptions();
  static final IncrementOptions DEFAULT_INCREMENT_OPTIONS = incrementOptions();
  static final DecrementOptions DEFAULT_DECREMENT_OPTIONS = decrementOptions();

  private final AsyncBinaryCollection async;
  private final Core core;

  ReactiveBinaryCollection(final Core core, final AsyncBinaryCollection async) {
    this.core = core;
    this.async = async;
  }

  public Mono<MutationResult> append(final String id, final byte[] content) {
    return append(id, content, DEFAULT_APPEND_OPTIONS);
  }

  public Mono<MutationResult> append(final String id, final byte[] content, final AppendOptions options) {
    return Mono.defer(() -> {
      notNull(options, "AppendOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      AppendOptions.Built opts = options.build();
      AppendRequest request = async.appendRequest(id, content, opts);
      return Reactor.wrap(
        request,
        AppendAccessor.append(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

  public Mono<MutationResult> prepend(final String id, final byte[] content) {
    return prepend(id, content, DEFAULT_PREPEND_OPTIONS);
  }

  public Mono<MutationResult> prepend(final String id, final byte[] content, final PrependOptions options) {
    return Mono.defer(() -> {
      notNull(options, "PrependOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      PrependOptions.Built opts = options.build();
      PrependRequest request = async.prependRequest(id, content, opts);
      return Reactor.wrap(
        request,
        PrependAccessor.prepend(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

  public Mono<CounterResult> increment(final String id) {
    return increment(id, DEFAULT_INCREMENT_OPTIONS);
  }

  public Mono<CounterResult> increment(final String id, final IncrementOptions options) {
    return Mono.defer(() -> {
      notNull(options, "IncrementOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      IncrementOptions.Built opts = options.build();
      IncrementRequest request = async.incrementRequest(id, opts);
      return Reactor.wrap(
        request,
        CounterAccessor.increment(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

  public Mono<CounterResult> decrement(final String id) {
    return decrement(id, DEFAULT_DECREMENT_OPTIONS);
  }

  public Mono<CounterResult> decrement(final String id, final DecrementOptions options) {
    return Mono.defer(() -> {
      notNull(options, "DecrementOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      DecrementOptions.Built opts = options.build();
      DecrementRequest request = async.decrementRequest(id, opts);
      return Reactor.wrap(
        request,
        CounterAccessor.decrement(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

}
