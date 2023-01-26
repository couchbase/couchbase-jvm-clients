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
import com.couchbase.client.core.error.InvalidArgumentException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

@Stability.Internal
public interface CoreKvOps {

  CoreAsyncResponse<CoreGetResult> getAsync(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  );

  default CoreGetResult getBlocking(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  ) {
    return getAsync(common, key, projections, withExpiry).toBlocking();
  }

  default Mono<CoreGetResult> getReactive(
      CoreCommonOptions common,
      String key,
      List<String> projections,
      boolean withExpiry
  ) {
    return Mono.defer(() -> getAsync(common, key, projections, withExpiry).toMono());
  }

  default void checkProjectionLimits(
      List<String> projections,
      boolean withExpiry
  ) {
    if (withExpiry) {
      if (projections.size() > 15) {
        throw InvalidArgumentException.fromMessage("Only a maximum of 16 fields can be "
            + "projected per request due to a server limitation (includes the expiration macro as one field).");
      }
    } else if (projections.size() > 16) {
      throw InvalidArgumentException.fromMessage("Only a maximum of 16 fields can be "
          + "projected per request due to a server limitation.");
    }
  }

  CoreAsyncResponse<CoreGetResult> getAndLockAsync(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  );

  default CoreGetResult getAndLockBlocking(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    return getAndLockAsync(common, key, lockTime).toBlocking();
  }

  default Mono<CoreGetResult> getAndLockReactive(
      CoreCommonOptions common,
      String key,
      Duration lockTime
  ) {
    return Mono.defer(() -> getAndLockAsync(common, key, lockTime).toMono());
  }

  CoreAsyncResponse<CoreGetResult> getAndTouchAsync(
      CoreCommonOptions common,
      String key,
      long expiration
  );

  default CoreGetResult getAndTouchBlocking(
      CoreCommonOptions common,
      String key,
      long expiration
  ) {
    return getAndTouchAsync(common, key, expiration).toBlocking();
  }

  default Mono<CoreGetResult> getAndTouchReactive(
      CoreCommonOptions common,
      String key,
      long expiration
  ) {
    return Mono.defer(() -> getAndTouchAsync(common, key, expiration).toMono());
  }

  CoreAsyncResponse<CoreMutationResult> insertAsync(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      long expiry
  );

  default CoreMutationResult insertBlocking(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      long expiry
  ) {
    return insertAsync(common, key, content, durability, expiry).toBlocking();
  }

  default Mono<CoreMutationResult> insertReactive(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      long expiry
  ) {
    return Mono.defer(() -> insertAsync(common, key, content, durability, expiry).toMono());
  }

  CoreAsyncResponse<CoreMutationResult> upsertAsync(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      long expiry,
      boolean preserveExpiry
  );

  default CoreMutationResult upsertBlocking(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      long expiry,
      boolean preserveExpiry
  ) {
    return upsertAsync(common, key, content, durability, expiry, preserveExpiry).toBlocking();
  }

  default Mono<CoreMutationResult> upsertReactive(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      CoreDurability durability,
      long expiry,
      boolean preserveExpiry
  ) {
    return Mono.defer(() -> upsertAsync(common, key, content, durability, expiry, preserveExpiry).toMono());
  }

  CoreAsyncResponse<CoreMutationResult> replaceAsync(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      long cas,
      CoreDurability durability,
      long expiry,
      boolean preserveExpiry
  );

  default CoreMutationResult replaceBlocking(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      long cas,
      CoreDurability durability,
      long expiry,
      boolean preserveExpiry
  ) {
    return replaceAsync(common, key, content, cas, durability, expiry, preserveExpiry).toBlocking();
  }

  default Mono<CoreMutationResult> replaceReactive(
      CoreCommonOptions common,
      String key,
      Supplier<CoreEncodedContent> content,
      long cas,
      CoreDurability durability,
      long expiry,
      boolean preserveExpiry
  ) {
    return Mono.defer(() -> replaceAsync(common, key, content, cas, durability, expiry, preserveExpiry).toMono());
  }

  CoreAsyncResponse<CoreMutationResult> removeAsync(
      CoreCommonOptions common,
      String key,
      long cas,
      CoreDurability durability
  );

  default CoreMutationResult removeBlocking(
      CoreCommonOptions common,
      String key,
      long cas,
      CoreDurability durability
  ) {
    return removeAsync(common, key, cas, durability).toBlocking();
  }

  default Mono<CoreMutationResult> removeReactive(
      CoreCommonOptions common,
      String key,
      long cas,
      CoreDurability durability
  ) {
    return Mono.defer(() -> removeAsync(common, key, cas, durability).toMono());
  }

  CoreAsyncResponse<CoreExistsResult> existsAsync(
      CoreCommonOptions common,
      String key
  );

  default CoreExistsResult existsBlocking(
      CoreCommonOptions common,
      String key
  ) {
    return existsAsync(common, key).toBlocking();
  }

  default Mono<CoreExistsResult> existsReactive(
      CoreCommonOptions common,
      String key
  ) {
    return Mono.defer(() -> existsAsync(common, key).toMono());
  }

  CoreAsyncResponse<CoreMutationResult> touchAsync(
      CoreCommonOptions common,
      String key,
      long expiry
  );

  default CoreMutationResult touchBlocking(
      CoreCommonOptions common,
      String key,
      long expiry
  ) {
    return touchAsync(common, key, expiry).toBlocking();
  }

  default Mono<CoreMutationResult> touchReactive(
      CoreCommonOptions common,
      String key,
      long expiry
  ) {
    return Mono.defer(() -> touchAsync(common, key, expiry).toMono());
  }

  CoreAsyncResponse<Void> unlockAsync(
      CoreCommonOptions common,
      String key,
      long cas
  );

  default void unlockBlocking(
      CoreCommonOptions common,
      String key,
      long cas
  ) {
    unlockAsync(common, key, cas).toBlocking();
  }

  default Mono<Void> unlockReactive(
      CoreCommonOptions common,
      String key,
      long cas
  ) {
    return Mono.defer(() -> unlockAsync(common, key, cas).toMono());
  }

  Flux<CoreGetResult> getAllReplicasReactive(
      CoreCommonOptions common,
      String key
  );

  Mono<CoreGetResult> getAnyReplicaReactive(
      CoreCommonOptions common,
      String key
  );

}
