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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

/**
 * Holds the results (including metadata) of a N1QL query.
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class QueryResult {

    private final List<QueryChunkRow> rows;
    private final QueryChunkHeader header;
    private final QueryChunkTrailer trailer;

    QueryResult(QueryChunkHeader header, List<QueryChunkRow> rows, QueryChunkTrailer trailer) {
        this.rows = rows;
        this.header = header;
        this.trailer = trailer;
    }

    /**
     * Returns all rows, converted into {@link JsonObject}s.
     * <p>
     * @throws DecodingFailedException if any row could not be successfully decoded
     */
    public Stream<JsonObject> rowsAsObject() {
        return rowsAs(JsonObject.class);
    }

    /**
     * Returns all rows, converted into the target class, and using the default decoder.
     * <p>
     * @param target the target class to decode into
     * @throws DecodingFailedException if any row could not be successfully decoded
     */
    public <T> Stream<T> rowsAs(final Class<T> target) {
        return rows.stream().map(n -> {
            try {
                return JacksonTransformers.MAPPER.readValue(n.data(), target);
            } catch (IOException ex) {
                throw new DecodingFailedException(ex);
            }
        });
    }

    public <T> List<T> allRowsAs(final Class<T> target) {
        return rowsAs(target).collect(Collectors.toList());
    }

    /**
     * Returns all rows, converted into {@link JsonObject}s.
     * <p>
     * @throws DecodingFailedException if any row could not be successfully decoded
     */
    public List<JsonObject> allRowsAsObject() {
        return allRowsAs(JsonObject.class);
    }

    /**
     * Returns a {@link QueryMetaData} giving access to the additional metadata associated with this query.
     */
    public QueryMetaData metaData() {
        return QueryMetaData.from(header, trailer);
    }

    @Override
    public String toString() {
        return "QueryResult{" +
                "rows=" + rows +
                ", header=" + header +
                ", trailer=" + trailer +
                '}';
    }
}