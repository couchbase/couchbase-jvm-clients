/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.classic.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.kv.CoreCounterResult;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.api.kv.CoreKvBinaryParamValidators;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.core.classic.ClassicHelper;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import com.couchbase.client.core.msg.kv.PrependRequest;
import com.couchbase.client.core.retry.RetryStrategy;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.classic.ClassicExpiryHelper.encode;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClassicCoreKvBinaryOps implements CoreKvBinaryOps {

  private final Core core;
  private final CoreKeyspace keyspace;
  private final Duration defaultKvTimeout;
  private final Duration defaultKvDurableTimeout;

  public ClassicCoreKvBinaryOps(Core core, CoreKeyspace keyspace) {
    this.core = requireNonNull(core);
    this.defaultKvTimeout = core.context().environment().timeoutConfig().kvTimeout();
    this.defaultKvDurableTimeout = core.context().environment().timeoutConfig().kvDurableTimeout();
    this.keyspace = requireNonNull(keyspace);
  }

  /**
   * Appends binary content to the document with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @param options custom options to customize the append behavior.
   * @return a {@link CoreMutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Override
  public CoreAsyncResponse<CoreMutationResult> appendAsync(final String id, final byte[] content,
                                                           final CoreCommonOptions options, final long cas, final CoreDurability durability) {
    AppendRequest request = appendRequestClassic(id, content, options, cas, durability);
    CompletableFuture<CoreMutationResult> future =  BinaryAccessor.append(core, request, id, keyspace, durability);
    return ClassicHelper.newAsyncResponse(request, future);
  }

  private AppendRequest appendRequestClassic(final String id, final byte[] content, final CoreCommonOptions options, long cas,
                                             final CoreDurability durability) {
    CoreKvBinaryParamValidators.validateAppendPrependArgs(id, keyspace, options, content, cas, durability);
    Duration timeout = timeout(options, durability);
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment().retryStrategy());
    RequestSpan span = coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_APPEND,
      options.parentSpan().orElse(null));
    AppendRequest request = new AppendRequest(timeout, context(), collectionIdentifier(), retryStrategy, id,
      content, cas, durability.levelIfSynchronous(), span);
    request.context().clientContext(options.clientContext());
    return request;
  }


  /**
   * Prepends binary content to the document with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @param options custom options to customize the prepend behavior.
   * @return a {@link CoreMutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Override
  public CoreAsyncResponse<CoreMutationResult> prependAsync(final String id, final byte[] content,
      final CoreCommonOptions options, final long cas, final CoreDurability durability) {
    PrependRequest request = prependRequestClassic(id, content, options, cas, durability);
    CompletableFuture<CoreMutationResult> future =  BinaryAccessor.prepend(core, request, id, keyspace, durability);
    return ClassicHelper.newAsyncResponse(request, future);
  }

  private PrependRequest prependRequestClassic(final String id, final byte[] content, final CoreCommonOptions options,
                                               long cas, final CoreDurability durability) {
    CoreKvBinaryParamValidators.validateAppendPrependArgs(id, keyspace, options, content,  cas, durability);
    Duration timeout = timeout(options, durability);
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment().retryStrategy());
    RequestSpan span = coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_PREPEND,
      options.parentSpan().orElse(null));
    PrependRequest request = new PrependRequest(timeout, context(), collectionIdentifier(), retryStrategy, id,
      content, cas, durability.levelIfSynchronous(), span);
    request.context().clientContext(options.clientContext());
    return request;
  }

  /**
   * Increments the counter document by one or the number defined in the options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to customize the increment behavior.
   * @return a {@link CoreCounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Override
  public CoreAsyncResponse<CoreCounterResult> incrementAsync(final String id, final CoreCommonOptions options, CoreExpiry expiry,
      long delta, Optional<Long> initial, CoreDurability durability) {
    IncrementRequest request = incrementRequestClassic(id, options, expiry, delta, initial, durability);
    CompletableFuture<CoreCounterResult> future =   BinaryAccessor.increment(core, request, id,
        keyspace, durability);
    return ClassicHelper.newAsyncResponse(request, future);
  }

  private IncrementRequest incrementRequestClassic(final String id, final CoreCommonOptions options, final CoreExpiry expiry,
                                                final long delta, final Optional<Long> initial, final CoreDurability durability) {
    CoreKvBinaryParamValidators.validateIncrementDecrementArgs(id, keyspace, options, expiry, delta, initial,
        durability);
    Duration timeout = timeout(options, durability);
    RetryStrategy retryStrategy = options.retryStrategy().orElse(environment().retryStrategy());
    RequestSpan span = coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_INCREMENT,
      options.parentSpan().orElse(null));

    IncrementRequest request = new IncrementRequest(timeout, context(), collectionIdentifier(), retryStrategy, id,
      delta, initial, encode(expiry), durability.levelIfSynchronous(), span);
    request.context().clientContext(options.clientContext());
    return request;
  }

  /**
   * Decrements the counter document by one or the number defined in the options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to customize the decrement behavior.
   * @return a {@link CoreCounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Override
  public CoreAsyncResponse<CoreCounterResult> decrementAsync(final String id, final CoreCommonOptions options, CoreExpiry expiry,
      long delta, Optional<Long> initial, CoreDurability durability) {
    notNull(options, "DecrementOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    DecrementRequest request = decrementRequestClassic(id, options, expiry, delta, initial, durability);
    CompletableFuture<CoreCounterResult> future =   BinaryAccessor.decrement(core, request, id,
        keyspace, durability);
    return ClassicHelper.newAsyncResponse(request, future);
  }

  private DecrementRequest decrementRequestClassic(final String id, final CoreCommonOptions opts, final CoreExpiry expiry,
                                                   final long delta, final Optional<Long> initial, final CoreDurability durability) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    Duration timeout = timeout(opts, durability);
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment().retryStrategy());
    RequestSpan span = coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_DECREMENT,
      opts.parentSpan().orElse(null));

    DecrementRequest request = new DecrementRequest(timeout, context(), collectionIdentifier(), retryStrategy, id,
      delta, initial, encode(expiry), durability.levelIfSynchronous(), span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  private CoreContext context() {
    return core.context();
  }

  private CoreEnvironment environment() {
    return core.context().environment();
  }

  private CoreResources coreResources() {
    return core.context().coreResources();
  }

  private CollectionIdentifier collectionIdentifier() {
    return keyspace.toCollectionIdentifier();
  }

  private Duration timeout(CoreCommonOptions common, CoreDurability durability) {
    return common.timeout().orElse(durability.isPersistent() ? defaultKvDurableTimeout : defaultKvTimeout);
  }
  
}
