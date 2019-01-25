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
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.PrependRequest;
import com.couchbase.client.java.kv.AppendAccessor;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependAccessor;
import com.couchbase.client.java.kv.PrependOptions;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.AsyncUtils.block;

public class ReactiveBinaryCollection {

  private final AsyncBinaryCollection async;
  private final Core core;

  ReactiveBinaryCollection(final Core core, final AsyncBinaryCollection async) {
    this.core = core;
    this.async = async;
  }

  public Mono<MutationResult> append(final String id, final byte[] content) {
    return append(id, content, AppendOptions.DEFAULT);
  }

  public Mono<MutationResult> append(final String id, final byte[] content,
                                     final AppendOptions options) {
    return Mono.defer(() -> {
      AppendRequest request = async.appendRequest(id, content, options);
      return Reactor.wrap(request, AppendAccessor.append(core, request), true);
    });
  }

  public Mono<MutationResult> prepend(final String id, final byte[] content) {
    return prepend(id, content, PrependOptions.DEFAULT);
  }

  public Mono<MutationResult> prepend(final String id, final byte[] content,
                                      final PrependOptions options) {
    return Mono.defer(() -> {
      PrependRequest request = async.prependRequest(id, content, options);
      return Reactor.wrap(request, PrependAccessor.prepend(core, request), true);
    });
  }

  public Mono<MutationResult> increment(final String id) {
    throw new UnsupportedOperationException("Not Implemented Yet");
  }

  public Mono<MutationResult> increment(final String id, final IncrementOptions options) {
    throw new UnsupportedOperationException("Not Implemented Yet");
  }

  public Mono<MutationResult> decrement(final String id) {
    throw new UnsupportedOperationException("Not Implemented Yet");
  }

  public Mono<MutationResult> decrement(final String id, final DecrementOptions options) {
    throw new UnsupportedOperationException("Not Implemented Yet");
  }

}
