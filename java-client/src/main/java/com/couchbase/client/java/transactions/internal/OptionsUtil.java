/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.transactions.TransactionQueryOptions;
import reactor.util.annotation.Nullable;

import java.io.IOException;

@Stability.Internal
public class OptionsUtil {
    private OptionsUtil() {}

    public static ObjectNode createTransactionOptions(@Nullable final ReactiveScope scope,
                                                      final String statement,
                                                      @Nullable final TransactionQueryOptions options) {
        JsonObject json = JsonObject.create()
                .put("statement", statement);
        if (scope != null) {
            json.put("query_context", QueryRequest.queryContext(scope.bucketName(), scope.name()));
        }
        if (options != null) {
            options.builder().build().injectParams(json);
        }
        try {
            return Mapper.reader().readValue(json.toBytes(), ObjectNode.class);
        } catch (IOException e) {
            throw new EncodingFailureException(e);
        }
    }

}
