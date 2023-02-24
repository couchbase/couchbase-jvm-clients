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
package com.couchbase.client.java.search.facet;

import com.couchbase.client.core.api.search.facet.CoreSearchFacet;
import com.couchbase.client.core.api.search.facet.CoreTermFacet;

/**
 * A facet that gives the number of occurrences of the most recurring terms in all rows.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class TermFacet extends SearchFacet {

    TermFacet(String field, int limit) {
        super(field, limit);
    }

    @Override
    public CoreSearchFacet toCore() {
        return new CoreTermFacet(field, size);
    }

}
