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

package com.couchbase.client.core.error;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.json.Mapper;

import java.nio.charset.StandardCharsets;

import static java.util.Objects.requireNonNull;

/**
 * There was a problem fulfilling the query request.
 *
 * Check <code>msg()</code> for further details.
 *
 * @author Graham Pople
 * @since 2.0.0
 */
public class QueryException extends CouchbaseException {
    private final byte[] content;
    private int code;
    private String msg;

    public QueryException(final byte[] content) {
        this.content = requireNonNull(content);

        try {
            final JsonNode node = Mapper.decodeIntoTree(content).path(0);
            code = node.path("code").asInt();
            msg = node.path("msg").asText();
        }
        catch (Exception e) {
            code = 0;
            msg = new String(content, StandardCharsets.UTF_8);
        }
    }

    /**
     * Returns a human-readable description of the error, as reported by the query service.
     */
    public String msg() {
        return msg;
    }

    /**
     * Returns the raw error code from the query service.
     * <p>
     * These are detailed in the
     * <a href="https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/n1ql-error-codes.html">
     * N1QL query codes documentation</a>.
     */
    public int code() {
        return code;
    }

    @Override
    public String getMessage() {
        return "Query Failed: " + new String(content, StandardCharsets.UTF_8);
    }
}
