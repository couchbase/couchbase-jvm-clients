/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.error.context;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import reactor.util.annotation.Nullable;

import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

@Stability.Uncommitted
public class ReducedSearchErrorContext extends ErrorContext {

  private final @Nullable String indexName;
  private final @Nullable CoreSearchQuery searchQuery;

  public ReducedSearchErrorContext(@Nullable String indexName, @Nullable CoreSearchQuery searchQuery) {
    super(null);
    this.indexName = indexName;
    this.searchQuery = searchQuery;
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (indexName != null) {
      input.put("indexName", redactMeta(indexName));
    }
    if (searchQuery != null) {
      input.put("query", redactUser(searchQuery.export()));
    }
  }

}
