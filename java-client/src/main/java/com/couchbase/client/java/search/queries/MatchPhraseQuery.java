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
import com.couchbase.client.core.api.search.queries.CoreMatchPhraseQuery;
import com.couchbase.client.java.search.SearchQuery;

/**
 * A FTS query that matches several given terms (a "phrase"), applying further processing
 * like analyzers to them.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class MatchPhraseQuery extends SearchQuery {

    private final String matchPhrase;
    private String field;
    private String analyzer;

    public MatchPhraseQuery(String matchPhrase) {
        super();
        this.matchPhrase = matchPhrase;
    }

    @Override
    public MatchPhraseQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    @Override
    public CoreSearchQuery toCore() {
        return new CoreMatchPhraseQuery(matchPhrase, field, analyzer, boost);
    }

    public MatchPhraseQuery field(String field) {
        this.field = field;
        return this;
    }

    public MatchPhraseQuery analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }
}
