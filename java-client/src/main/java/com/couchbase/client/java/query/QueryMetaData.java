/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.query;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;
import java.util.Optional;

/**
 * Stores any non-rows results related to the execution of a particular N1QL query.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public abstract class QueryMetaData {

    /**
     * Returns the request identifier string of the query request
     */
    public abstract String requestId();

    /**
     * Returns the client context identifier string set on the query request.
     */
    public abstract String clientContextId();

    /**
     * Returns the raw query execution status as returned by the query engine
     */
    public abstract QueryStatus status();

    /**
     * Returns the signature as returned by the query engine which is then decoded to {@link JsonObject}
     * <p>
     * It is returned as an Optional which will be empty if no signature information is available.
     *
     * @throws DecodingFailureException when the signature cannot be decoded successfully
     */
    public abstract Optional<JsonObject> signature();

    /**
     * Returns the profiling information returned by the query engine which is then decoded to {@link JsonObject}
     * <p>
     * It is returned as an Optional which will be empty if no profile information is available.
     *
     * @throws DecodingFailureException when the profile cannot be decoded successfully
     */
    public abstract Optional<JsonObject> profile();

    /**
     * Returns the {@link QueryMetrics} as returned by the query engine if enabled.
     *
     * @throws DecodingFailureException when the metrics cannot be decoded successfully
     */
    public abstract Optional<QueryMetrics> metrics();

    /**
     * Returns any warnings returned by the query engine, as a {@link JsonArray}.
     * <p>
     * It is returned as an Optional which will be empty if no warnings were returned
     *
     * @throws DecodingFailureException when the warnings cannot be decoded successfully
     */
    public abstract List<QueryWarning> warnings();
}
