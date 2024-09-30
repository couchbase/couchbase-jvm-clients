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

import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.util.CoreIdGenerator;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class ProtostellarContext extends AbstractContext {
  private final long id = CoreIdGenerator.nextId();
  private final String hexId = String.format("0x%016x", id);

  private final CoreEnvironment env;
  private final Authenticator authenticator;
  private final CoreResources coreResources;

  public ProtostellarContext(final CoreEnvironment env, final Authenticator authenticator, final CoreResources coreResources) {
    this.env = requireNonNull(env);
    this.authenticator = requireNonNull(authenticator);
    this.coreResources = requireNonNull(coreResources);

    if (env.securityConfig().tlsEnabled() && !authenticator.supportsTls()) {
      throw InvalidArgumentException.fromMessage("TLS enabled but the Authenticator does not support TLS!");
    } else if (!env.securityConfig().tlsEnabled() && !authenticator.supportsNonTls()) {
      throw InvalidArgumentException.fromMessage("TLS not enabled but the Authenticator requires TLS!");
    }
  }

  public long id() {
    return id;
  }

  public String hexId() {
    return hexId;
  }

  public CoreEnvironment environment() {
    return env;
  }

  public CoreResources coreResources() {
    return coreResources;
  }

  public Authenticator authenticator() {
    return authenticator;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    input.put("coreId", hexId);
  }

}
