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
package com.couchbase.client.core.api.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Optional;

@Stability.Internal
public interface CoreKvBinaryOps {

  default CoreMutationResult appendBlocking(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    return appendAsync(id, content, options, cas, durability).toBlocking();
  }

  CoreAsyncResponse<CoreMutationResult> appendAsync(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability);

  default Mono<CoreMutationResult> appendReactive(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    return Mono.defer(() -> appendAsync(id, content, options, cas, durability).toMono());
  }

  default CoreMutationResult prependBlocking(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    return prependAsync(id, content, options, cas, durability).toBlocking();
  }

  CoreAsyncResponse<CoreMutationResult> prependAsync(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durabilityLevel);

  default Mono<CoreMutationResult> prependReactive(String id, byte[] content, CoreCommonOptions options, long cas,
      CoreDurability durability) {
    return Mono.defer(() -> prependAsync(id, content, options, cas, durability).toMono());
  }

  default CoreCounterResult incrementBlocking(String id, CoreCommonOptions options, CoreExpiry expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    return incrementAsync(id, options, expiry, delta, initial, durability).toBlocking();
  }

  CoreAsyncResponse<CoreCounterResult> incrementAsync(String id, CoreCommonOptions options, CoreExpiry expiry, long delta,
      Optional<Long> initial, CoreDurability durability);

  default Mono<CoreCounterResult> incrementReactive(String id, CoreCommonOptions options, CoreExpiry expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    return Mono.defer(() -> incrementAsync(id, options, expiry, delta, initial, durability).toMono());
  }

  default CoreCounterResult decrementBlocking(String id, CoreCommonOptions options, CoreExpiry expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    return decrementAsync(id, options, expiry, delta, initial, durability).toBlocking();
  }

  CoreAsyncResponse<CoreCounterResult> decrementAsync(String id, CoreCommonOptions options, CoreExpiry expiry, long delta,
      Optional<Long> initial, CoreDurability durability);

  default Mono<CoreCounterResult> decrementReactive(String id, CoreCommonOptions options, CoreExpiry expiry, long delta,
      Optional<Long> initial, CoreDurability durability) {
    return Mono.defer(() -> decrementAsync(id, options, expiry, delta, initial, durability).toMono());
  }

}
