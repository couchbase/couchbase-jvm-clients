/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreDurability;
import com.couchbase.client.core.cnc.CbTracing;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.google.protobuf.Timestamp;
import com.couchbase.client.core.deps.io.grpc.Deadline;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.msg.kv.CodecFlags;
import com.couchbase.client.core.retry.ProtostellarRequestBehaviour;
import com.couchbase.client.protostellar.kv.v1.DocumentContentType;
import com.couchbase.client.protostellar.kv.v1.DurabilityLevel;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Stability.Internal
public class CoreProtostellarUtil {
  private CoreProtostellarUtil() {}

  public static Duration kvTimeout(Optional<Duration> customTimeout, Core core) {
    return customTimeout.orElse(core.context().environment().timeoutConfig().kvTimeout());
  }

  public static Duration kvDurableTimeout(Optional<Duration> customTimeout,
                                          CoreDurability dl,
                                          Core core) {
    if (customTimeout.isPresent()) {
      return customTimeout.get();
    } else if (dl.isLegacy()) {
      throw new FeatureNotAvailableException("Legacy durability is not supported with Protostellar; please use Durability instead");
    } else if (!dl.isNone()) {
      return core.context().environment().timeoutConfig().kvDurableTimeout();
    } else {
      return core.context().environment().timeoutConfig().kvTimeout();
    }
  }

  public static Duration kvDurableTimeout(Optional<Duration> customTimeout,
                                          Optional<com.couchbase.client.core.msg.kv.DurabilityLevel> dl,
                                          Core core) {
    if (customTimeout.isPresent()) {
      return customTimeout.get();
    } else if (dl.isPresent()) {
      return core.context().environment().timeoutConfig().kvDurableTimeout();
    } else {
      return core.context().environment().timeoutConfig().kvTimeout();
    }
  }

  public static Deadline convertTimeout(Optional<Duration> customTimeout, Duration defaultTimeout) {
    if (customTimeout.isPresent()) {
      return Deadline.after(customTimeout.get().toMillis(), TimeUnit.MILLISECONDS);
    } else {
      return Deadline.after(defaultTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  public static Deadline convertTimeout(Duration timeout) {
    return Deadline.after(timeout.toMillis(), TimeUnit.MILLISECONDS);
  }

  public static Duration managementTimeout(Optional<Duration> customTimeout, Core core) {
    return customTimeout.orElse(core.context().environment().timeoutConfig().managementTimeout());
  }

  public static int convertToFlags(DocumentContentType contentType) {
    int flags = 0;
    switch (contentType) {
      case JSON:
        flags = CodecFlags.JSON_COMPAT_FLAGS;
        break;
      case BINARY:
        flags = CodecFlags.BINARY_COMPAT_FLAGS;
        break;
    }
    return flags;
  }

  public static DocumentContentType convertFromFlags(int flags) {
    if ((flags & CodecFlags.JSON_COMPAT_FLAGS) != 0) {
      return DocumentContentType.JSON;
    }
    if ((flags & CodecFlags.BINARY_COMPAT_FLAGS) != 0) {
      return DocumentContentType.BINARY;
    }
    return DocumentContentType.UNKNOWN;
  }

  public static void handleShutdownBlocking(Core core, ProtostellarRequest<?> request) {
    if (core.protostellar().endpoint().isShutdown()) {
      throw RequestCanceledException.shuttingDown(request.context());
    }
  }

  public static <T> boolean handleShutdownAsync(Core core, CompletableFuture<T> ret, ProtostellarRequest<?> request) {
    if (core.protostellar().endpoint().isShutdown()) {
      ret.completeExceptionally(RequestCanceledException.shuttingDown(request.context()));
      return true;
    }
    return false;
  }

  public static <TSdkResult> boolean handleShutdownReactive(Sinks.One<TSdkResult> ret, Core core, ProtostellarRequest<?> request) {
    if (core.protostellar().endpoint().isShutdown()) {
      ret.tryEmitError(RequestCanceledException.shuttingDown(request.context())).orThrow();
      return true;
    }
    return false;
  }

  public static <T> @Nullable Mono<T> handleShutdownReactive(Core core, ProtostellarRequest<?> request) {
    if (core.protostellar().endpoint().isShutdown()) {
      return Mono.error(RequestCanceledException.shuttingDown(request.context()));
    }
    return null;
  }

  public static DurabilityLevel convert(com.couchbase.client.core.msg.kv.DurabilityLevel dl) {
    switch (dl) {
      case MAJORITY:
        return DurabilityLevel.MAJORITY;
      case MAJORITY_AND_PERSIST_TO_ACTIVE:
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case PERSIST_TO_MAJORITY:
        return DurabilityLevel.PERSIST_TO_MAJORITY;
    }

    // NONE should be handled earlier, by not sending anything.
    throw new IllegalArgumentException("Unknown durability level " + dl);
  }

  public static DurabilityLevel convert(CoreDurability dl) {
    if (dl.isNone()) {
      throw new IllegalStateException("Should not have no durability here");
    }

    switch (dl.levelIfSynchronous().get()) {
      case MAJORITY:
        return DurabilityLevel.MAJORITY;
      case MAJORITY_AND_PERSIST_TO_ACTIVE:
        return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
      case PERSIST_TO_MAJORITY:
        return DurabilityLevel.PERSIST_TO_MAJORITY;
    }

    // NONE should be handled earlier, by not sending anything.
    throw new IllegalArgumentException("Unknown durability level " + dl);
  }

  public static @Nullable Instant convertExpiry(boolean hasExpiry, Timestamp expiry) {
    if (hasExpiry) {
      return Instant.ofEpochSecond(expiry.getSeconds());
    }
    return null;
  }

  public static Timestamp convertExpiry(long expiry) {
    return Timestamp.newBuilder().setSeconds(expiry).build();
  }

  public static <TResponse> ProtostellarRequestBehaviour convertKeyValueException(Core core,
                                                                                  ProtostellarRequest<TResponse> request,
                                                                                  Throwable t) {
    return CoreProtostellarErrorHandlingUtil.convertKeyValueException(core, request, t);
  }

  public static RequestSpan createSpan(Core core,
                                       String spanName,
                                       CoreDurability durability,
                                       @Nullable RequestSpan parent) {
    RequestSpan span = CbTracing.newSpan(core.context().environment().requestTracer(), spanName, parent);

    if (!durability.isNone() && !durability.isLegacy()) {
      switch (durability.levelIfSynchronous().get()) {
        case MAJORITY:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "majority");
          break;
        case MAJORITY_AND_PERSIST_TO_ACTIVE:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "majority_and_persist_active");
          break;
        case PERSIST_TO_MAJORITY:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "persist_majority");
          break;
      }
    }

    return span;
  }

  public static RuntimeException unsupportedInProtostellar(String feature) {
    return new FeatureNotAvailableException("Feature '" + feature + "' is not supported when using protostellar:// to connect");
  }

  // JVMCBC-1187: This and everything using it will be fixed and removed before GA.
  public static RuntimeException unsupportedCurrentlyInProtostellar() {
    return new FeatureNotAvailableException("Feature is not supported when using protostellar:// to connect (but will be before GA)");
  }
}
