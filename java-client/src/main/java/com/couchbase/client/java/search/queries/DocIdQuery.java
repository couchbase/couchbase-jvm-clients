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
import com.couchbase.client.core.api.search.queries.CoreDocIdQuery;
import com.couchbase.client.java.search.SearchQuery;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * A FTS query that matches on Couchbase document IDs. Useful to restrict the search space to a list of keys
 * (by using this in a {@link AbstractCompoundQuery compound query}).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class DocIdQuery extends SearchQuery {

    private final List<String> docIds = new LinkedList<String>();

    public DocIdQuery(String... docIds) {
        super();
        Collections.addAll(this.docIds, docIds);
    }

    public DocIdQuery docIds(String... docIds) {
        Collections.addAll(this.docIds, docIds);
        return this;
    }

    @Override
    public DocIdQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    @Override
    public CoreSearchQuery toCore() {
        return new CoreDocIdQuery(boost, docIds);
    }
}
