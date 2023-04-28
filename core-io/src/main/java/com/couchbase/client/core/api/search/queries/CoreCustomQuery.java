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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.ProtostellarUtil;
import com.couchbase.client.core.util.Validators;
import com.couchbase.client.protostellar.search.v1.BooleanQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.protostellar.CoreProtostellarUtil.unsupportedInProtostellar;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Collections.unmodifiableMap;

@Stability.Internal
public class CoreCustomQuery extends CoreSearchQuery {
  private final Map<String, Object> params;

  public CoreCustomQuery(Map<String, Object> customQuery, @Nullable Double boost) {
    super(boost);
    Validators.notNull(customQuery, "custom query");
    this.params = unmodifiableMap(new HashMap<>(customQuery));
  }

  @Override
  protected void injectParams(ObjectNode input) {
    ObjectNode obj = Mapper.convertValue(params, ObjectNode.class);
    obj.fields().forEachRemaining(field -> input.set(field.getKey(), field.getValue()));
  }

  @Override
  public Query asProtostellar() {
    throw unsupportedInProtostellar("custom search queries");
  }
}
