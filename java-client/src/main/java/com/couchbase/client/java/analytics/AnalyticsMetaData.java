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

package com.couchbase.client.java.analytics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkHeader;
import com.couchbase.client.core.msg.analytics.AnalyticsChunkTrailer;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Holds associated metadata returned by the server for the performed analytics request.
 */
public class AnalyticsMetaData {

    private final AnalyticsChunkHeader header;
    private final AnalyticsChunkTrailer trailer;

    private AnalyticsMetaData(final AnalyticsChunkHeader header, final AnalyticsChunkTrailer trailer) {
        this.header = header;
        this.trailer = trailer;
    }

    @Stability.Internal
    static AnalyticsMetaData from(final AnalyticsChunkHeader header, final AnalyticsChunkTrailer trailer) {
        return new AnalyticsMetaData(header, trailer);
    }

    /**
     * Get the request identifier of the query request
     *
     * @return request identifier
     */
    public String requestId() {
        return header.requestId();
    }

    /**
     * Get the client context identifier as set by the client
     *
     * @return client context identifier
     */
    public String clientContextId() {
        return header.clientContextId().orElse("");
    }

    /**
     * Get the status of the response.
     *
     * @return the status of the response.
     */
    public AnalyticsStatus status() {
        return AnalyticsStatus.from(trailer.status());
    }

    /**
     * Get the signature as the target type, if present.
     *
     * @return the decoded signature if present.
     */
    public Optional<JsonObject> signature() {
        return header.signature().map(bytes -> {
            try {
                return JacksonTransformers.MAPPER.readValue(bytes, JsonObject.class);
            } catch (IOException e) {
                throw new DecodingFailureException("Could not decode Analytics signature", e);
            }
        });
    }

    /**
     * Get the associated metrics for the response.
     *
     * @return the metrics for the analytics response.
     */
    public AnalyticsMetrics metrics() {
        return new AnalyticsMetrics(trailer.metrics());
    }

    /**
     * Returns warnings if present.
     *
     * @return warnings, if present.
     */
    public List<AnalyticsWarning> warnings() {
        return this.trailer.warnings().map(warnings ->
          ErrorCodeAndMessage.fromJsonArray(warnings).stream().map(AnalyticsWarning::new).collect(Collectors.toList())
        ).orElse(Collections.emptyList());
    }

    @Override
    public String toString() {
        return "AnalyticsMetaData{" +
          "header=" + header +
          ", trailer=" + trailer +
          '}';
    }
}
