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
import com.couchbase.client.core.api.search.queries.CoreWildcardQuery;
import com.couchbase.client.java.search.SearchQuery;

/**
 * An FTS query that allows for simple matching using wildcard characters (* and ?).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class WildcardQuery extends SearchQuery {

    private final String wildcard;
    private String field;

    public WildcardQuery(String wildcard) {
        super();
        this.wildcard = wildcard;
    }

    public WildcardQuery field(String field) {
        this.field = field;
        return this;
    }

    @Override
    public WildcardQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    @Override
    public CoreSearchQuery toCore() {
        return new CoreWildcardQuery(wildcard, field, boost);
    }
}
