/*
 * Copyright (c) 2026 Couchbase, Inc.
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

package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.util.annotation.Nullable;

/**
 * Empty strings, zero and empty maps are treated as unset.
 */
@Stability.Internal
public final class CoreModelOptions {
  private final @Nullable String provider;
  private final @Nullable String name;
  private final @Nullable String endpoint;
  private final @Nullable String region;
  private final @Nullable Integer outputTokenLimit;
  private final @Nullable Boolean moderation;
  private final @Nullable ObjectNode raw;

  public CoreModelOptions(@Nullable String provider,
                          @Nullable String name,
                          @Nullable String endpoint,
                          @Nullable String region,
                          @Nullable Integer outputTokenLimit,
                          @Nullable Boolean moderation,
                          @Nullable ObjectNode raw) {
    this.provider = emptyToNull(provider);
    this.name = emptyToNull(name);
    this.endpoint = emptyToNull(endpoint);
    this.region = emptyToNull(region);
    this.outputTokenLimit = outputTokenLimit == null || outputTokenLimit == 0 ? null : outputTokenLimit;
    this.moderation = moderation;
    this.raw = raw == null || raw.isEmpty() ? null : raw;
  }

  private static @Nullable String emptyToNull(@Nullable String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  public @Nullable String provider() {
    return provider;
  }

  public @Nullable String name() {
    return name;
  }

  public @Nullable String endpoint() {
    return endpoint;
  }

  public @Nullable String region() {
    return region;
  }

  public @Nullable Integer outputTokenLimit() {
    return outputTokenLimit;
  }

  public @Nullable Boolean moderation() {
    return moderation;
  }

  public @Nullable ObjectNode raw() {
    return raw;
  }

  @Override
  public String toString() {
    // raw is deliberately excluded, as it may contain secrets.
    return "CoreModelOptions{" +
        "provider=" + provider +
        ", name=" + name +
        ", endpoint=" + endpoint +
        ", region=" + region +
        ", outputTokenLimit=" + outputTokenLimit +
        ", moderation=" + moderation +
        ", raw=" + (raw == null ? "null" : "<redacted>") +
        '}';
  }
}
