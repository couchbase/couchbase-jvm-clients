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

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class CoreGetResult extends CoreKvResult {
  private final byte[] content;
  private final int flags;
  private final long cas;
  @Nullable private final Instant expiry;
  private final boolean replica;

  public CoreGetResult(
      @Nullable CoreKvResponseMetadata meta,
      CoreKeyspace keyspace,
      String key,
      byte[] content,
      int flags,
      long cas,
      @Nullable Instant expiry,
      boolean replica
  ) {
    super(keyspace, key, meta);
    this.content = requireNonNull(content);
    this.flags = flags;
    this.cas = cas;
    this.expiry = expiry;
    this.replica = replica;
  }

  public byte[] content() {
    return content;
  }

  public int flags() {
    return flags;
  }

  public long cas() {
    return cas;
  }

  @Nullable
  public Instant expiry() {
    return expiry;
  }

  public boolean replica() {
    return replica;
  }
}
