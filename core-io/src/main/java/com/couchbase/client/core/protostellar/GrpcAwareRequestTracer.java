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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.io.grpc.ManagedChannelBuilder;

@Stability.Internal
public interface GrpcAwareRequestTracer {

  /**
   * Setup GRPC instrumentation on a given GRPC channel.
   */
  void registerGrpc(ManagedChannelBuilder<?> builder);

  /**
   * Puts `span` into ThreadLocalStorage, ready to be picked up by libraries that rely on that mechanism and can't be passed the span explicitly.
   * <p>
   * We require this as asynchronous mechanisms such as reactive and CompletableFuture do not play well with ThreadLocalStorage.
   */
  AutoCloseable activateSpan(RequestSpan span);
}
