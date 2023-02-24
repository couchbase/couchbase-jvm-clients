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
import com.couchbase.client.protostellar.search.v1.MatchAllQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

@Stability.Internal
public class CoreMatchAllQuery extends CoreSearchQuery {

    public CoreMatchAllQuery(@Nullable Double boost) {
        super(boost);
    }

    @Override
    protected void injectParams(ObjectNode input) {
        input.put("match_all", (String) null);
    }

    @Override
    public Query asProtostellar() {
        return Query.newBuilder().setMatchAllQuery(MatchAllQuery.getDefaultInstance()).build();
    }
}
