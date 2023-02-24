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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.DocIdQuery;
import com.couchbase.client.protostellar.search.v1.Query;

import java.util.List;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CoreDocIdQuery extends CoreSearchQuery {

  private final List<String> docIds;

  public CoreDocIdQuery(Double boost, List<String> docIds) {
    super(boost);
    this.docIds = notNullOrEmpty(docIds, "Document IDs");
  }

  @Override
  protected void injectParams(ObjectNode input) {
    ArrayNode ids = Mapper.createArrayNode();
    docIds.forEach(ids::add);
    input.set("ids", ids);
  }

  @Override
  public Query asProtostellar() {
    DocIdQuery.Builder builder = DocIdQuery.newBuilder()
            .addAllIds(docIds);

    if (boost != null) {
      builder.setBoost(boost.floatValue());
    }

    return Query.newBuilder().setDocIdQuery(builder).build();
  }
}
