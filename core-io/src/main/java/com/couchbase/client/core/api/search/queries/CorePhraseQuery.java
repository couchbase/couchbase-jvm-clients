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
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.protostellar.search.v1.PhraseQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import java.util.List;

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

@Stability.Internal
public class CorePhraseQuery extends CoreSearchQuery {

    private final List<String> terms;
    private final @Nullable String field;

    public CorePhraseQuery(List<String> terms, @Nullable String field, @Nullable Double boost) {
        super(boost);
        this.terms = notNullOrEmpty(terms, "Terms");
        this.field = field;
    }

    @Override
    protected void injectParams(ObjectNode input) {
        ArrayNode json = Mapper.createArrayNode();
        terms.forEach(json::add);
        input.set("terms", json);

        if (field != null) {
            input.put("field", this.field);
        }
    }

    @Override
    public Query asProtostellar() {
        PhraseQuery.Builder builder = PhraseQuery.newBuilder()
                .addAllTerms(terms);

        if (field != null) {
            builder.setField(field);
        }

        if (boost != null) {
            builder.setBoost(boost.floatValue());
        }

        return Query.newBuilder().setPhraseQuery(builder).build();
    }
}
