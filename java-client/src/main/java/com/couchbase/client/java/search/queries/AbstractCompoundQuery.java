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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.java.search.SearchQuery;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for FTS queries that are composite, compounding several other {@link SearchQuery}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Internal
public abstract class AbstractCompoundQuery extends SearchQuery {

    List<SearchQuery> childQueries = new LinkedList<SearchQuery>();

    protected AbstractCompoundQuery(SearchQuery... queries) {
        super();
        addAll(queries);
    }

    protected void addAll(SearchQuery... queries) {
        Collections.addAll(childQueries, queries);
    }

    public List<SearchQuery> childQueries() {
        return this.childQueries;
    }

    protected List<CoreSearchQuery> childQueriesAsCore() {
        return childQueries.stream().map(SearchQuery::toCore).collect(Collectors.toList());
    }
}
