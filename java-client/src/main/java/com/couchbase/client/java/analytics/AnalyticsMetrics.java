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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.json.JacksonTransformers;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Holds the metrics as returned from an analytics response.
 */
public class AnalyticsMetrics {

    /**
     * Provides a pointer into the root nodes of the raw response for easier decoding.
     */
    private final JsonNode rootNode;

    /**
     * Creates new {@link AnalyticsMetrics} from the raw data.
     *
     * @param raw the raw analytics data.
     */
    AnalyticsMetrics(final byte[] raw) {
        try {
            this.rootNode = JacksonTransformers.MAPPER.readTree(raw);
        } catch (IOException e) {
            throw new DecodingFailureException("Could not parse analytics metrics!");
        }
    }

    /**
     * @return The total time taken for the request, that is the time from when the
     * request was received until the results were returned, in a human-readable
     * format (eg. 123.45ms for a little over 123 milliseconds).
     */
    public Duration elapsedTime() {
        return decode(String.class, "elapsedTime").map(Golang::parseDuration).orElse(Duration.ZERO);
    }

    /**
     * @return The time taken for the execution of the request, that is the time from
     * when query execution started until the results were returned, in a human-readable
     * format (eg. 123.45ms for a little over 123 milliseconds).
     */
    public Duration executionTime() {
        return decode(String.class, "executionTime").map(Golang::parseDuration).orElse(Duration.ZERO);
    }

    /**
     * @return The total number of objects in the results.
     */
    public long resultCount() {
        return decode(Long.class, "resultCount").orElse(0L);
    }

    /**
     * @return The total number of bytes in the results.
     */
    public long resultSize() {
        return decode(Long.class, "resultSize").orElse(0L);
    }

    /**
     * @return The number of errors that occurred during the request.
     */
    public long errorCount() {
        return decode(Long.class, "errorCount").orElse(0L);
    }

    /**
     * @return The number of warnings that occurred during the request.
     */
    public long warningCount() {
        return decode(Long.class, "warningCount").orElse(0L);
    }

    /**
     * @return The number of processed objects for the request.
     */
    public long processedObjects() {
        return decode(Long.class, "processedObjects").orElse(0L);
    }

    /**
     * Helper method to turn a given path of the raw data into the target class.
     *
     * @param target the target class to decode into.
     * @param path the path of the raw json.
     * @param <T> the generic type to decide into.
     * @return the generic decoded object if present and not null.
     */
    private <T> Optional<T> decode(final Class<T> target, final String path) {
        try {
            JsonNode subNode = rootNode.path(path);
            if (subNode == null || subNode.isNull() || subNode.isMissingNode()) {
                return Optional.empty();
            }
            return Optional.ofNullable(JacksonTransformers.MAPPER.treeToValue(subNode, target));
        } catch (JsonProcessingException e) {
            throw new DecodingFailureException("Could not decode " + path + " in analytics metrics!");
        }
    }

    @Override
    public String toString() {
        return "AnalyticsMetrics{" +
            "raw=" + redactUser(Mapper.encodeAsString(rootNode)) +
            '}';
    }

}
