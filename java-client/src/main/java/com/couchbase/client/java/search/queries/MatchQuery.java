/*
 * Copyright (c) 2016 Couchbase, Inc.
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
package com.couchbase.client.java.search.queries;

import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreMatchOperator;
import com.couchbase.client.core.api.search.queries.CoreMatchQuery;
import com.couchbase.client.java.search.SearchQuery;

/**
 * A FTS query that matches a given term, applying further processing to it
 * like analyzers, stemming and even {@link #fuzziness(int) fuzziness}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class MatchQuery extends SearchQuery {

    private final String match;
    private String field;
    private String analyzer;
    private Integer prefixLength;
    private Integer fuzziness;
    private CoreMatchOperator operator;

    public MatchQuery(String match) {
        super();
        this.match = match;
    }

    @Override
    public MatchQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    public MatchQuery field(String field) {
        this.field = field;
        return this;
    }

    public MatchQuery analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public MatchQuery prefixLength(int prefixLength) {
        this.prefixLength = prefixLength;
        return this;
    }

    public MatchQuery fuzziness(int fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    public MatchQuery operator(final MatchOperator operator) {
        this.operator = operator == MatchOperator.OR ? CoreMatchOperator.OR : CoreMatchOperator.AND;
        return this;
    }

    @Override
    public CoreSearchQuery toCore() {
        return new CoreMatchQuery(match, field, analyzer, prefixLength, fuzziness, operator, boost);
    }
}
