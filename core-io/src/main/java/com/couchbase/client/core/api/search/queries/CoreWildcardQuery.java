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
package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.protostellar.search.v1.Query;
import com.couchbase.client.protostellar.search.v1.WildcardQuery;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreWildcardQuery extends CoreSearchQuery {

  private final String wildcard;
  private final @Nullable String field;

  public CoreWildcardQuery(String wildcard, @Nullable String field, @Nullable Double boost) {
    super(boost);
    this.wildcard = notNull(wildcard, "Wildcard");
    this.field = field;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    if (field != null) {
      input.put("field", field);
    }
    input.put("wildcard", wildcard);
  }

  @Override
  public Query asProtostellar() {
    WildcardQuery.Builder builder = WildcardQuery.newBuilder()
            .setWildcard(wildcard);

    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setWildcardQuery(builder).build();
  }
}
