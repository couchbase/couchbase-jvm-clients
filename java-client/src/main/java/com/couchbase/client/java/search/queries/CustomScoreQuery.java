/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.search.queries;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.api.search.queries.CoreCustomScoreQuery;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.SearchQuery;
import reactor.util.annotation.Nullable;

import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.Validators.notNull;

/**
 * A search query that passes the results of an inner query through
 * a user-defined JavaScript scoring function, which assigns a new score to each result.
 * <p>
 * The function takes two parameters ({@code doc} and {@code params})
 * and returns a number: the new score of the document. For example:
 * <pre>
 * function hotel_score(doc, params) {
 *   const maxPrice = params.max_price || 1000;
 *   const f = doc.fields || {};
 *   const price = (f.price !== undefined) ? f.price : maxPrice;
 *   return doc.score * (2.0 - Math.min(price / maxPrice, 1.0));
 * }
 * </pre>
 * The function can access a document field only if the field is listed in {@link #fields(String...)}.
 * <p>
 * Custom script queries are disabled by default. A server administrator must enable them
 * by setting the Search service's {@code customScriptQueriesEnabled} manager option to {@code "true"}.
 *
 * @see SearchQuery#customScore(SearchQuery, String)
 */
@SinceCouchbase("8.5")
public class CustomScoreQuery extends SearchQuery {

    private final SearchQuery query;
    private final String source;
    private @Nullable List<String> fields;
    private @Nullable JsonObject parameters;

    public CustomScoreQuery(SearchQuery query, String source) {
        super();
        this.query = notNull(query, "Query");
        this.source = notNull(source, "Source");
    }

    /**
     * Specifies the document fields made available to the function.
     * Pass {@code "*"} to make all fields available.
     * <p>
     * If not set, the function does not have access to any document fields.
     *
     * @return this query, for chaining.
     */
    public CustomScoreQuery fields(String... fields) {
        this.fields = listOf(fields);
        return this;
    }

    /**
     * Specifies parameters made available to the function via its {@code params} argument.
     *
     * @return this query, for chaining.
     */
    public CustomScoreQuery parameters(JsonObject parameters) {
        this.parameters = notNull(parameters, "Parameters");
        return this;
    }

    /**
     * Has no effect. Custom script queries do not support boost.
     *
     * @return this query, for chaining.
     * @deprecated Custom script queries do not support boost.
     */
    @Deprecated
    @Override
    public CustomScoreQuery boost(double boost) {
        return this;
    }

    @Override
    public CoreCustomScoreQuery toCore() {
        return new CoreCustomScoreQuery(
            query.toCore(),
            source,
            fields,
            parameters == null ? null : parameters.toMap()
        );
    }
}
