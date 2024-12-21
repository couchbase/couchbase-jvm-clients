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
package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.protostellar.GrpcAwareRequestTracer;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

@Stability.Internal
// JVMCBC-1192: candidate for removal: will probably not be required when everything is moved to Core*Ops
public class ProtostellarUtil {
  private ProtostellarUtil() {}

  public static com.couchbase.client.core.deps.com.google.protobuf.Duration convert(java.time.Duration input) {
    return com.couchbase.client.core.deps.com.google.protobuf.Duration.newBuilder()
      .setSeconds(input.getSeconds())
      .setNanos(input.getNano())
      .build();
  }

  public static java.time.Duration convert(com.couchbase.client.core.deps.com.google.protobuf.Duration input) {
    return java.time.Duration.ofSeconds(input.getSeconds(), input.getNanos());
  }

  public static @Nullable AutoCloseable activateSpan(Optional<RequestSpan> parentSpan, @Nullable RequestSpan span, @Nullable RequestTracer tracer) {
    if (tracer instanceof GrpcAwareRequestTracer) {
      if (span != null) {
        return ((GrpcAwareRequestTracer) tracer).activateSpan(span);
      }
    }

    return null;
  }
}
