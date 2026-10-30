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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;
import reactor.util.annotation.Nullable;

import java.util.Optional;

@Stability.Internal
public final class CoreTokenUsage {
  private final @Nullable Integer promptTokens;
  private final @Nullable Integer completionTokens;
  private final @Nullable Integer totalTokens;

  public CoreTokenUsage(@Nullable Integer promptTokens, @Nullable Integer completionTokens, @Nullable Integer totalTokens) {
    this.promptTokens = promptTokens;
    this.completionTokens = completionTokens;
    this.totalTokens = totalTokens;
  }

  public static CoreTokenUsage fromJson(byte[] json) {
    JsonNode node = Mapper.decodeIntoTree(json);
    return new CoreTokenUsage(intOrNull(node, "promptTokens"), intOrNull(node, "completionTokens"), intOrNull(node, "totalTokens"));
  }

  private static @Nullable Integer intOrNull(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value != null && value.isNumber() ? value.intValue() : null;
  }

  public Optional<Integer> promptTokens() {
    return Optional.ofNullable(promptTokens);
  }

  public Optional<Integer> completionTokens() {
    return Optional.ofNullable(completionTokens);
  }

  public Optional<Integer> totalTokens() {
    return Optional.ofNullable(totalTokens);
  }

  @Override
  public String toString() {
    return "CoreTokenUsage{" +
        "promptTokens=" + promptTokens +
        ", completionTokens=" + completionTokens +
        ", totalTokens=" + totalTokens +
        '}';
  }
}
