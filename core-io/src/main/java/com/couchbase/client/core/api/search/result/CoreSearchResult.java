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

package com.couchbase.client.core.api.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchMetaData;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

@Stability.Internal
public class CoreSearchResult {
    private final List<CoreSearchRow> rows;
    private final CoreSearchMetaData meta;
    private final Map<String, CoreSearchFacetResult> facets;

    public CoreSearchResult(final List<CoreSearchRow> rows, final Map<String, CoreSearchFacetResult> facets, final CoreSearchMetaData meta) {
        this.rows = rows;
        this.facets = facets;
        this.meta = meta;
    }

    public CoreSearchMetaData metaData() {
        return meta;
    }
    public List<CoreSearchRow> rows() {
        return rows;
    }
    public Map<String, CoreSearchFacetResult> facets() {
        return facets;
    }

    @Override
    public String toString() {
        return "SearchResult{" +
          "rows=" + redactUser(rows) +
          ", meta=" + redactMeta(meta) +
          ", facets=" + redactUser(facets) +
          '}';
    }
}
