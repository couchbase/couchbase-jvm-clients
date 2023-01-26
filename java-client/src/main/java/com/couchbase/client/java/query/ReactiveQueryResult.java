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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive result that fetch parts of the N1ql Query responses reactively
 *
 * @since 3.0.0
 */
public abstract class ReactiveQueryResult {

	/**
	 * The default serializer to use.
	 */
	protected final JsonSerializer serializer;

	@Stability.Internal
	public ReactiveQueryResult(final JsonSerializer serializer) {
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

	/**
	 * Get a {@link Flux} which publishes the rows that were fetched by the query which are then decoded to the
	 * requested entity class
	 *
	 * @param target target class for converting the query row
	 * @return {@link Flux}
   * @throws DecodingFailureException (async) if the decoding cannot be completed successfully
	 */
	public abstract <T> Flux<T> rowsAs(Class<T> target);

  /**
   * Get a {@link Flux} which publishes the rows that were fetched by the query which are then decoded to the
   * requested entity type
   *
   * @param target target type for converting the query row
   * @return {@link Flux}
   * @throws DecodingFailureException (async) if the decoding cannot be completed successfully
   */
	public abstract <T> Flux<T> rowsAs(TypeRef<T> target);

	/**
	 * Returns a {@link Mono} containing a {@link QueryMetaData},  giving access to the additional metadata associated with
	 * this query.
	 *
	 * Note that the metadata will only be available once all rows have been received, so it is recommended that you
	 * first handle the rows in your code, and then the metadata.  This will avoid buffering all the rows in-memory.
	 */
	public abstract Mono<QueryMetaData> metaData();
}
