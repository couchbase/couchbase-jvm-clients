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
import com.couchbase.client.core.msg.kv.MutationToken;
import reactor.util.annotation.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@Stability.Internal
public class CoreMutationResult extends CoreKvResult {
  private final long cas;
  private final Optional<MutationToken> mutationToken;

  public CoreMutationResult(
      @Nullable CoreKvResponseMetadata meta,
      CoreKeyspace keyspace,
      String key,
      long cas,
      Optional<MutationToken> mutationToken
  ) {
    super(keyspace, key, meta);
    this.cas = cas;
    this.mutationToken = requireNonNull(mutationToken);
  }

  public long cas() {
    return cas;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }
}
