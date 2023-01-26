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

import static java.util.Objects.requireNonNull;

@Stability.Internal
public abstract class CoreKvResult {
  private final CoreKeyspace keyspace;
  private final String key;
  private final CoreKvResponseMetadata meta;

  public CoreKvResult(CoreKeyspace keyspace, String key, @Nullable CoreKvResponseMetadata meta) {
    this.keyspace = requireNonNull(keyspace);
    this.key = requireNonNull(key);
    this.meta = meta == null ? CoreKvResponseMetadata.NONE : meta;
  }

  public CoreKeyspace keyspace() {
    return keyspace;
  }

  public String key() {
    return key;
  }

  public CoreKvResponseMetadata meta() {
    return meta;
  }
}
