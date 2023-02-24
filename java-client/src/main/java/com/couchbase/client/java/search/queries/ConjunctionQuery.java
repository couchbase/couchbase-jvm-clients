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
import com.couchbase.client.core.api.search.queries.CoreConjunctionQuery;
import com.couchbase.client.java.search.SearchQuery;

/**
 * A compound FTS query that performs a logical AND between all its sub-queries (conjunction).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class ConjunctionQuery extends AbstractCompoundQuery {

    public ConjunctionQuery(SearchQuery... queries) {
        super(queries);
    }

    @Override
    public ConjunctionQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    public ConjunctionQuery and(SearchQuery... queries) {
        super.addAll(queries);
        return this;
    }

    @Override
    public CoreConjunctionQuery toCore() {
        return new CoreConjunctionQuery(childQueriesAsCore(), boost);
    }
}
