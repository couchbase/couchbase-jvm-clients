/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.vector;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import reactor.util.annotation.Nullable;

import java.util.Locale;

@Stability.Internal
public class CoreVectorSearchOptions {
  private final @Nullable CoreVectorQueryCombination vectorQueryCombination;

  public CoreVectorSearchOptions(@Nullable CoreVectorQueryCombination vectorQueryCombination) {
    this.vectorQueryCombination = vectorQueryCombination;
  }

  public void injectTopLevel(ObjectNode toSend) {
    if (vectorQueryCombination != null) {
      toSend.put("knn_operator", vectorQueryCombination.name().toLowerCase(Locale.ROOT));
    }
  }
}
