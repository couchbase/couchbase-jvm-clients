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

	private final QueryResponse response;

	@Stability.Internal
	public ReactiveQueryResult(QueryResponse response) {
		this.response = response;
	}

	/**
	 * Returns the request identifier string of the query request
	 */
	public String requestId() {
		return this.response.header().requestId();
	}

	/**
	 * Returns the client context identifier string set on the query request, if available
	 */
	public Optional<String> clientContextId() {
		return this.response.header().clientContextId();
	}

	/**
	 * Gets a {@link Mono} which publishes the {@link QueryMetrics} as returned by the query engine
	 *
	 * @return {@link Mono}
	 */
//	public Mono<QueryMetrics> info() {
//		return this.response.metrics().map(n -> {
//			try {
//				JsonObject jsonObject = JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
//				return new QueryMetrics(jsonObject);
//			} catch (IOException ex) {
//				throw new CouchbaseException(ex);
//			}
//		});
//	}
	// TODO

	/**
	 * Get a {@link Mono} which publishes the query execution status as returned by the query engine
	 *
	 * @return {@link Mono}
	 */
//	public Mono<String> queryStatus() {
//		return this.response.queryStatus();
//	}

	/**
	 * Get a {@link Mono} which publishes the signature as returned by the query engine
	 *
	 * @return {@link Mono}
	 */
//	public Mono<JsonObject> signature() {
//		return this.response.signature().map(n -> {
//			try {
//				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
//			} catch (IOException ex) {
//				throw new CouchbaseException(ex);
//			}
//		});
//	}

	/**
	 * Get a {@link Mono} which publishes the profile info as returned by the query engine
	 *
	 * @return {@link Mono}
	 */
//	public Mono<JsonObject> profileInfo() {
//		return this.response.profile().map(n -> {
//			try {
//				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
//			} catch (IOException ex) {
//				throw new CouchbaseException(ex);
//			}
//		});
//	}

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
	public <T>Flux<T> rows(Class<T> target) {
		return this.response.rows().map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n.data(), target);
			} catch (IOException ex) {
				throw new CouchbaseException(ex);
			}
		});
	}

	/**
	 * Get a {@link Flux} which publishes the query execution warnings as returned by the query engine which are then
	 * decoded to {@link JsonObject}
	 *
	 * The flux can complete successfully or throw
	 * - {@link DecodingFailedException } when the decoding cannot be completed successfully
	 *
	 * @return {@link Flux}
	 */
//	public Flux<JsonObject> warnings() {
//		return this.response.warnings().map(n -> {
//			try {
//				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
//			} catch (IOException ex) {
//				throw new CouchbaseException(ex);
//			}
//		});
//	}

	/**
	 * Get a {@link Flux} which publishes query execution errors as returned by the query engine which are then decoded
	 * to {@link JsonObject}
	 *
	 * The flux can complete successfully or throw
	 * - {@link DecodingFailedException } when the decoding cannot be completed successfully
	 *
	 * @return {@link Flux}
	 */
//	public Flux<JsonObject> errors() {
//		return this.response.errors().map(n -> {
//			try {
//				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
//			} catch (IOException ex) {
//				throw new IllegalStateException(ex);
//			}
//		});
//	}
}