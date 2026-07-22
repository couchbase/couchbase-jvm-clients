/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.topology.ClusterCapability;
import org.jspecify.annotations.Nullable;

@Stability.Internal
public abstract class CoreSearchScoring {
  public final @Nullable ClusterCapability requiredCapability;

  public CoreSearchScoring() {
    this(null);
  }

  public CoreSearchScoring(@Nullable ClusterCapability requiredCapability) {
    this.requiredCapability = requiredCapability;
  }

  public abstract void inject(ObjectNode queryJson);

  public static class Disabled extends CoreSearchScoring {
    public static final Disabled INSTANCE = new Disabled();

    public void inject(ObjectNode queryJson) {
      queryJson.put("score", "none");
    }
  }

  public static class RelativeScoreFusion extends CoreSearchScoring {
    public @Nullable Integer windowSize;

    public RelativeScoreFusion(@Nullable Integer windowSize) {
      super(ClusterCapability.SEARCH_SCORE_FUSION);
      this.windowSize = windowSize;
    }

    public void inject(ObjectNode queryJson) {
      queryJson.put("score", "rsf");
      setParamIfNotNull(queryJson, "score_window_size", windowSize);
    }
  }

  public static class ReciprocalRankFusion extends CoreSearchScoring {
    public @Nullable Integer windowSize;
    public @Nullable Integer rankConstant;

    public ReciprocalRankFusion(
        @Nullable Integer windowSize,
        @Nullable Integer rankConstant
    ) {
      super(ClusterCapability.SEARCH_SCORE_FUSION);
      this.windowSize = windowSize;
      this.rankConstant = rankConstant;
    }

    public void inject(ObjectNode queryJson) {
      queryJson.put("score", "rrf");
      setParamIfNotNull(queryJson, "score_window_size", windowSize);
      setParamIfNotNull(queryJson, "score_rank_constant", rankConstant);
    }
  }

  public static void setParamIfNotNull(
      ObjectNode queryJson,
      String fieldName,
      @Nullable Object value
  ) {
    if (value == null) return;
    ObjectNode params = getOrCreateObject(queryJson, "params");
    params.set(fieldName, Mapper.convertValue(value, JsonNode.class));
  }

  private static ObjectNode getOrCreateObject(ObjectNode node, String fieldName) {
    ObjectNode child = (ObjectNode) node.get(fieldName);
    if (child == null) {
      child = Mapper.createObjectNode();
      node.set(fieldName, child);
    }
    return child;
  }
}
