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

package com.couchbase.client.java.search.result;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.SearchMetaData;

import java.util.List;

/**
 * Full Text Search (FTS) query results, as returned from the asynchronous API.
 *
 * It is also an {@link Iterable Iterable&lt;SearchQueryRow&gt;}, where iteration is similar to iterating over
 * {@link #rowsOrFail()}.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class SearchResult {

    private final List<SearchQueryRow> rows;
    private final List<RuntimeException> errors;
    private final SearchMetaData meta;

    public SearchResult(List<SearchQueryRow> rows,
                        List<RuntimeException> errors,
                        SearchMetaData meta) {
        this.rows = rows;
        this.errors = errors;
        this.meta = meta;
    }

    /**
     * Any additional meta information associated with the FTS query.
     */
    public SearchMetaData metaData() {
        return meta;
    }

    /**
     * The list of FTS result rows for the FTS query. This method always returns
     * a list, including when an execution error (eg. partial results) occurred.
     *
     * @see #rowsOrFail() for a variant that throws an exception whenever execution errors have occurred.
     * @see #errors() to get a list of execution errors in JSON form.
     */
    public List<SearchQueryRow> rows() {
        return rows;
    }

    /**
     * The list of FTS result rows for the FTS query. In case of an execution error
     * (eg. partial results), a {@link RuntimeException} is thrown instead, containing the first error.
     *
     * @see #rows() for a variant that lists partial results instead of throwing the exception.
     * @see #errors() to get a list of execution errors in JSON form.
     */
    public List<SearchQueryRow> rowsOrFail() {
        if (errors.isEmpty()) {
            return rows();
        }
        else {
            throw errors.get(0);
        }
    }

    /**
     * When an execution error happens (including partial results), this method returns a {@link List} of
     * the error(s) in {@link JsonObject JSON format}.
     *
     * @see #rows() to get results, including partial results (rather than throwing an exception).
     * @see #rowsOrFail() to get only full results, but throwing an exception whenever execution errors have occurred.
     */
    public List<RuntimeException> errors() {
        return errors;
    }
}
