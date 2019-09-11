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

import com.couchbase.client.core.msg.analytics.AnalyticsResponse;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveAnalyticsResult {

    private final AnalyticsResponse response;

    /**
     * The default serializer to use.
     */
    private final JsonSerializer serializer;

    ReactiveAnalyticsResult(final AnalyticsResponse response, final JsonSerializer serializer) {
        this.response = response;
        this.serializer = serializer;
    }

    /**
     * Get a {@link Flux} which publishes the rows that were fetched by the query which are then decoded to
     * {@link JsonObject}
     *
     * @return {@link Flux}
     */
    public Flux<JsonObject> rowsAsObject() {
        return rowsAs(JsonObject.class);
    }

    public <T> Flux<T> rowsAs(final Class<T> target) {
        return response.rows().map(row -> serializer.deserialize(target, row.data()));
    }

    public Mono<AnalyticsMetaData> metaData() {
        return response.trailer().map(t -> AnalyticsMetaData.from(response.header(), t));
    }
}
