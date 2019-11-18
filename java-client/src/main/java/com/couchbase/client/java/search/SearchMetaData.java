/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.java.search;

import com.couchbase.client.java.search.result.SearchMetrics;

import java.util.Map;

public class SearchMetaData {
    private final Map<String, String> errors;
    private final SearchMetrics metrics;

    SearchMetaData(final Map<String, String> errors, final SearchMetrics metrics) {
        this.errors = errors;
        this.metrics = metrics;
    }

    /**
     * Provides a {@link SearchMetrics} object giving statistics on the request like number of rows, total time taken...
     */
    public SearchMetrics metrics() {
        return metrics;
    }

    public Map<String, String> errors() {
        return errors;
    }

    @Override
    public String toString() {
        return "SearchMeta{" +
          "metrics=" + metrics +
          ", errors=" + errors +
          '}';
    }
}
