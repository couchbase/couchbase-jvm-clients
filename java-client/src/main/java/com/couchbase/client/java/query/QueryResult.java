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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.EncodedDocument;

/**
 * Query Result that fetches the parts of the Query response asynchronously
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class QueryResult {

    private final Stream<QueryChunkRow> rows;
    private final QueryMeta meta;

    @Stability.Internal
    public QueryResult(Stream<QueryChunkRow> rows,
                            QueryMeta meta) {
        this.rows = rows;
        this.meta = meta;
    }

    /**
     * Returns all rows, converted into {@link JsonObject}s.
     * <p>
     * @throws DecodingFailedException if any row could not be successfully decoded
     */
    public Stream<JsonObject> rows() {
        return rows(JsonObject.class);
    }

    /**
     * Returns all rows, converted into the target class, and using the default decoder.
     * <p>
     * @param target the target class to decode into
     * @throws DecodingFailedException if any row could not be successfully decoded
     */
    public <T> Stream<T> rows(Class<T> target) {
        return this.rows.map(n -> {
            try {
                return JacksonTransformers.MAPPER.readValue(n.data(), target);
            } catch (IOException ex) {
                throw new DecodingFailedException(ex);
            }
        });
    }

    /**
     * Returns all rows, converted into the target class, using a custom decoder.
     * <p>
     * @param target the target class to decode into
     * @param decoder the customer {@link Decoder} to use
     * @throws DecodingFailedException if any row could not be successfully decoded
     */
    public <T> Stream<T> rows(Class<T> target, Decoder<T> decoder) {
        return this.rows.map(n -> decoder.decode(target, EncodedDocument.of(0, n.data())));
    }

    /**
     * Returns a {@link QueryMeta} giving access to the additional metadata associated with this query.
     */
    public QueryMeta meta() {
        return meta;
    }
}