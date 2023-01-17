/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.ProtostellarEndpoint;
import com.couchbase.client.core.endpoint.ProtostellarPool;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.InvalidArgumentException;

import java.time.Duration;
import java.util.Set;

@Stability.Internal
public class CoreProtostellar {
  /**
   * The default port used for Protostellar.
   */
  public static final int DEFAULT_PROTOSTELLAR_TLS_PORT = 18098;


  private final ProtostellarPool pool;
  private final Set<SeedNode> seedNodes;
  private final Core core;

  public CoreProtostellar(final Core core, final Authenticator authenticator, final Set<SeedNode> seedNodes) {
    if (core.context().environment().securityConfig().tlsEnabled() && !authenticator.supportsTls()) {
      throw new InvalidArgumentException("TLS enabled but the Authenticator does not support TLS!", null, null);
    } else if (!core.context().environment().securityConfig().tlsEnabled() && !authenticator.supportsNonTls()) {
      throw new InvalidArgumentException("TLS not enabled but the Authenticator does only support TLS!", null, null);
    }

    if (seedNodes.isEmpty()) {
      throw new IllegalStateException("Have no seed nodes");
    }

    this.seedNodes = seedNodes;
    this.core = core;
    SeedNode first = seedNodes.stream().findFirst().get();
    this.pool = new ProtostellarPool(core, first.address(), first.protostellarPort().orElse(DEFAULT_PROTOSTELLAR_TLS_PORT));
  }

  public void shutdown(final Duration timeout) {
    pool.shutdown(timeout);
  }

  public ProtostellarEndpoint endpoint() {
    return pool.endpoint();
  }

  public ProtostellarPool pool() {
    return pool;
  }
}
