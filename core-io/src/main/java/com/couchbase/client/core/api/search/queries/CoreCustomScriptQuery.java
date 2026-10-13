/*
 * Copyright (c) 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.topology.ClusterCapability;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableList;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;

/**
 * Base class for search queries that pass the results of an inner query
 * through a user-defined JavaScript function.
 */
@Stability.Internal
public abstract class CoreCustomScriptQuery extends CoreSearchQuery {
  private static final Set<ClusterCapability> REQUIRED_CAPABILITIES = singleton(ClusterCapability.SEARCH_UDF_QUERY);

  public final CoreSearchQuery query;
  public final String source;
  public final List<String> fields;
  public final Map<String, @Nullable Object> params;
  private final List<CoreSearchQuery> childQueries;

  protected CoreCustomScriptQuery(
      CoreSearchQuery query,
      String source,
      @Nullable List<String> fields,
      @Nullable Map<String, ?> params
  ) {
    super(null); // The server ignores boost for custom script queries
    this.query = notNull(query, "Query");
    this.source = notNull(source, "Source");
    this.fields = copyToUnmodifiableList(fields);
    this.params = isNullOrEmpty(params) ? emptyMap() : unmodifiableMap(new HashMap<>(params));
    this.childQueries = singletonList(this.query);
  }

  /**
   * The name of the JSON field that holds this query's settings.
   */
  protected abstract String queryType();

  @Override
  protected void injectParams(ObjectNode input) {
    ObjectNode json = Mapper.createObjectNode();
    json.set("query", query.export());
    json.put("source", source);
    if (!fields.isEmpty()) {
      json.set("fields", Mapper.convertValue(fields, ArrayNode.class));
    }
    if (!params.isEmpty()) {
      json.set("params", Mapper.convertValue(params, ObjectNode.class));
    }
    input.set(queryType(), json);
  }

  @Override
  public List<CoreSearchQuery> childQueries() {
    return childQueries;
  }

  @Override
  protected Set<ClusterCapability> ownRequiredCapabilities() {
    return REQUIRED_CAPABILITIES;
  }
}
