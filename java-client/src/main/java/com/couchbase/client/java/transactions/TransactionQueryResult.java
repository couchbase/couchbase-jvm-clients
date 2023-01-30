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

package com.couchbase.client.java.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryChunkTrailer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The result of a N1QL query executed within a transaction, including rows and associated metadata.
 * <p>
 * Queries executed inside a transaction are always blocking/non-streaming, to allow essential error handling logic to
 * reliably take place.
 */
public class TransactionQueryResult {
   private final CoreQueryResult internal;

    /**
     * The default serializer to use.
     */
    private final JsonSerializer serializer;

    /**
     * Creates a new TransactionQueryResult.
     */
    @Stability.Internal
    public TransactionQueryResult(CoreQueryResult internal,
                                  final JsonSerializer serializer) {
        this.internal = Objects.requireNonNull(internal);
        this.serializer = Objects.requireNonNull(serializer);
    }

    /**
     * Returns all rows, converted into {@link JsonObject}s.
     *
     * @throws DecodingFailureException if any row could not be successfully deserialized.
     */
    public List<JsonObject> rowsAsObject() {
        return rowsAs(JsonObject.class);
    }

    /**
     * Returns all rows, converted into instances of the target class.
     *
     * @param target the target class to deserialize into.
     * @throws DecodingFailureException if any row could not be successfully deserialized.
     */
    public <T> List<T> rowsAs(final Class<T> target) {
        List<QueryChunkRow> rows = internal.collectRows();
        final List<T> converted = new ArrayList<>(rows.size());
        for (QueryChunkRow row : rows) {
            converted.add(serializer.deserialize(target, row.data()));
        }
        return converted;
    }

    /**
     * Returns all rows, converted into instances of the target type.
     *
     * @param target the target type to deserialize into.
     * @throws DecodingFailureException if any row could not be successfully deserialized.
     */
    public <T> List<T> rowsAs(final TypeRef<T> target) {
        List<QueryChunkRow> rows = internal.collectRows();
        final List<T> converted = new ArrayList<>(rows.size());
        for (QueryChunkRow row : rows) {
            converted.add(serializer.deserialize(target, row.data()));
        }
        return converted;
    }

    /**
     * Returns the {@link QueryMetaData} giving access to the additional metadata associated with this query.
     */
    public QueryMetaData metaData() {
        return new QueryMetaData(internal.metaData());
    }
}
