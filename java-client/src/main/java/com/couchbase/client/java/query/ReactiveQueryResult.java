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
import java.util.Optional;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive result that fetch parts of the N1ql Query responses reactively
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class ReactiveQueryResult {

	private final Flux<QueryChunkRow> rows;
	private final Mono<QueryMeta> meta;

	@Stability.Internal
	public ReactiveQueryResult(Flux<QueryChunkRow> rows,
							   Mono<QueryMeta> meta) {
		this.rows = rows;
		this.meta = meta;
	}

	/**
	 * Get a {@link Flux} which publishes the rows that were fetched by the query which are then decoded to
	 * {@link JsonObject}
	 *
	 * @return {@link Flux}
	 */
	public Flux<JsonObject> rows() {
		return rows(JsonObject.class);
	}

	/**
	 * Get a {@link Flux} which publishes the rows that were fetched by the query which are then decoded to the
	 * requested entity class
	 *
	 *
	 * The flux can complete successfully or throw
	 * - {@link DecodingFailedException } when the decoding cannot be completed successfully
	 *
	 * @param target target class for converting the query row
	 * @return {@link Flux}
	 */
	public <T> Flux<T> rows(Class<T> target) {
		return this.rows.map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n.data(), target);
			} catch (IOException ex) {
				throw new CouchbaseException(ex);
			}
		});
	}

	/**
	 * Returns a {@link Mono} containing a {@link QueryMeta},  giving access to the additional metadata associated with
	 * this query.
	 *
	 * Note that the metadata will only be available once all rows have been received, so it is recommended that you
	 * first handle the rows in your code, and then the metadata.  This will avoid buffering all the rows in-memory.
	 */
	public Mono<QueryMeta> meta() {
		return meta;
	}

}