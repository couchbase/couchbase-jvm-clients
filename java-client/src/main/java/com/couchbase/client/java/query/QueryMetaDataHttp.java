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

package com.couchbase.client.java.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Stores any non-rows results related to the execution of a particular N1QL query.
 *
 * @author Graham Pople
 * @since 1.0.0
 */
public class QueryMetaDataHttp extends QueryMetaData {

    private final QueryChunkHeader header;
    private final QueryChunkTrailer trailer;

    @Stability.Internal
    private QueryMetaDataHttp(QueryChunkHeader header, QueryChunkTrailer trailer) {
        this.header = header;
        this.trailer = trailer;
    }

    @Stability.Internal
    public static QueryMetaDataHttp from(final QueryChunkHeader header, final QueryChunkTrailer trailer) {
        return new QueryMetaDataHttp(header, trailer);
    }

    /**
     * Returns the request identifier string of the query request
     */
    public String requestId() {
        return header.requestId();
    }

    /**
     * Returns the client context identifier string set on the query request.
     */
    public String clientContextId() {
        return header.clientContextId().orElse("");
    }

    /**
     * Returns the raw query execution status as returned by the query engine
     */
    public QueryStatus status() {
        return QueryStatus.from(trailer.status());
    }

    /**
     * Returns the signature as returned by the query engine which is then decoded to {@link JsonObject}
     * <p>
     * It is returned as an Optional which will be empty if no signature information is available.
     *
     * @throws DecodingFailureException when the signature cannot be decoded successfully
     */
    public Optional<JsonObject> signature() {
        return header.signature().map(v -> {
            try {
                return JacksonTransformers.MAPPER.readValue(v, JsonObject.class);
            } catch (IOException ex) {
                throw new DecodingFailureException(ex);
            }
        });
    }

    /**
     * Returns the profiling information returned by the query engine which is then decoded to {@link JsonObject}
     * <p>
     * It is returned as an Optional which will be empty if no profile information is available.
     *
     * @throws DecodingFailureException when the profile cannot be decoded successfully
     */
    public Optional<JsonObject> profile() {
        return trailer.profile().map(profile -> {
            try {
                return JacksonTransformers.MAPPER.readValue(profile, JsonObject.class);
            } catch (IOException ex) {
                throw new DecodingFailureException(ex);
            }
        });
    }

    /**
     * Returns the {@link QueryMetrics} as returned by the query engine if enabled.
     *
     * @throws DecodingFailureException when the metrics cannot be decoded successfully
     */
    public Optional<QueryMetrics> metrics() {
        return trailer.metrics().map(QueryMetricsHttp::new);
    }

    /**
     * Returns any warnings returned by the query engine, as a {@link JsonArray}.
     * <p>
     * It is returned as an Optional which will be empty if no warnings were returned
     *
     * @throws DecodingFailureException when the warnings cannot be decoded successfully
     */
    public List<QueryWarning> warnings() {
        return this.trailer.warnings().map(warnings ->
          ErrorCodeAndMessage.fromJsonArray(warnings).stream().map(QueryWarning::new).collect(Collectors.toList())
        ).orElse(Collections.emptyList());
    }

    @Override
    public String toString() {
        return "QueryMetaData{" +
          "header=" + header +
          ", trailer=" + trailer +
          '}';
    }
}
