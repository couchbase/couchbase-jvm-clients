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
import com.couchbase.client.protostellar.search.v1.BooleanFieldQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreBooleanFieldQuery extends CoreSearchQuery {

  private final boolean value;
  private final @Nullable String field;

  public CoreBooleanFieldQuery(boolean value, @Nullable String field, @Nullable Double boost) {
    super(boost);
    this.value = value;
    this.field = field;
  }

  @Override
  protected void injectParams(ObjectNode input) {
    if (field != null) {
      input.put("field", field);
    }

    input.put("bool", value);
  }

  @Override
  public Query asProtostellar() {
    BooleanFieldQuery.Builder builder = BooleanFieldQuery.newBuilder()
            .setValue(value);

    if (field != null) {
      builder.setField(field);
    }

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setBooleanFieldQuery(builder).build();
  }
}
