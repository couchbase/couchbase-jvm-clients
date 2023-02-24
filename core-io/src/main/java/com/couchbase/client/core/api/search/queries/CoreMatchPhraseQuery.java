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
import com.couchbase.client.protostellar.search.v1.MatchPhraseQuery;
import com.couchbase.client.protostellar.search.v1.Query;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class CoreMatchPhraseQuery extends CoreSearchQuery {

    private final String matchPhrase;
    private @Nullable final String field;
    private @Nullable final String analyzer;

    public CoreMatchPhraseQuery(String matchPhrase, @Nullable String field, @Nullable String analyzer, @Nullable Double boost) {
        super(boost);
        this.matchPhrase = notNull(matchPhrase, "Match phrase");
        this.field = field;
        this.analyzer = analyzer;
    }

    @Override
    public void injectParams(ObjectNode input) {
        input.put("match_phrase", matchPhrase);
        if (field != null) {
            input.put("field", field);
        }
        if (analyzer != null) {
            input.put("analyzer", analyzer);
        }
    }

    @Override
    public Query asProtostellar() {
        MatchPhraseQuery.Builder builder = MatchPhraseQuery.newBuilder()
                .setPhrase(matchPhrase);

        if (boost != null) {
            builder.setBoost(boost.floatValue());
        }

        if (field != null) {
          builder.setField(field);
        }

        if (analyzer != null) {
          builder.setAnalyzer(analyzer);
        }

        return Query.newBuilder().setMatchPhraseQuery(builder).build();
    }
}
