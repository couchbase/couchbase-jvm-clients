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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailedException;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;

/**
 * Query Result that fetches the parts of the Query response asynchronously
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class AsyncQueryResult {

	private final QueryResponse response;

	@Stability.Internal
	public AsyncQueryResult(QueryResponse response) {
		this.response = response;
	}

	/**
	 * Returns the request identifier string of the query request
	 */
	public String requestId() {
		return this.response.requestId();
	}

	/**
	 * Returns the client context identifier string set on the query request, if it's available
	 */
	public Optional<String> clientContextId() {
		return this.response.clientContextId();
	}

	/**
	 * A {@link CompletableFuture} which completes with the query execution status as returned by the query engine
	 *
	 * @return {@link CompletableFuture}
	 */
	public CompletableFuture<String> queryStatus() {
		return this.response.additional()
				.map(v -> v.status)
				.toFuture();
	}

	/**
	 * A {@link CompletableFuture} which completes with the signature as returned by the query
	 * engine which are then decoded to {@link JsonObject}
	 *
	 * The future can complete successfully or throw an {@link ExecutionException} wrapping
	 * - {@link DecodingFailedException } when the request cannot be completed successfully
	 *
	 * @return {@link CompletableFuture}
	 */
	public JsonObject signature() {
		return response.signature().map(v -> {
			try {
				return JacksonTransformers.MAPPER.readValue(v, JsonObject.class);
			} catch (IOException ex) {
				throw new DecodingFailedException(ex);
			}
		}).orElseThrow(QueryResponse::errorSignatureNotPresent);
	}

	/**
	 * A {@link CompletableFuture} which completes with profiling information as returned by the query
	 * engine which are then decoded to {@link JsonObject}
	 *
	 * The future can complete successfully or throw an {@link ExecutionException} wrapping
	 * - {@link DecodingFailedException } when the request cannot be completed successfully
	 *
	 * @return {@link CompletableFuture}
	 */
	public CompletableFuture<JsonObject> profileInfo() {
		return this.response.additional().map(addl -> {
			if (addl.profile.isPresent()) {
				try {
					return JacksonTransformers.MAPPER.readValue(addl.profile.get(), JsonObject.class);
				} catch (IOException ex) {
					throw new DecodingFailedException(ex);
				}
			}
			else {
				throw QueryResponse.errorProfileNotPresent();
			}
		}).toFuture();
	}

	/**
	 * A {@link CompletableFuture} which completes with the the {@link QueryMetrics} as returned by the query engine
	 *
	 * The future can complete successfully or throw an {@link ExecutionException} wrapping
	 * - {@link DecodingFailedException} when the decoding of the part of the response failed
	 *
	 * @return {@link CompletableFuture}
	 */
	public CompletableFuture<QueryMetrics> metrics() {
		return this.response.additional().map(addl -> {
			try {
				JsonObject jsonObject = JacksonTransformers.MAPPER.readValue(addl.metrics, JsonObject.class);
				return new QueryMetrics(jsonObject);
			} catch (IOException ex) {
				throw new DecodingFailedException(ex);
			}
		}).toFuture();
	}

	/**
	 * A {@link CompletableFuture} which completes with the query execution status as returned by the query engine
	 *
	 * The future can complete successfully or throw an {@link ExecutionException} wrapping
	 * - {@link DecodingFailedException} when the decoding cannot be completed successfully
	 *
	 * @return {@link CompletableFuture}
	 */
	public CompletableFuture<List<JsonObject>> rows() {
		return rows(JsonObject.class);
	}

	/**
	 * A {@link CompletableFuture} which completes with the query execution status as returned by the query engine
	 *
	 * The future can complete successfully or throw an {@link ExecutionException} wrapping
	 * - {@link DecodingFailedException} when the decoding cannot be completed successfully
	 *
	 * @return {@link CompletableFuture}
	 */
	public <T> CompletableFuture<List<T>> rows(Class<T> target) {
		return this.response.rows().map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n, target);
			} catch (IOException ex) {
				throw new DecodingFailedException(ex);
			}
		}).collectList().toFuture();
	}

	/**
	 * A {@link CompletableFuture} which completes with the list of query execution warnings as returned by the query
	 * engine which are then decoded to {@link JsonObject}
	 *
	 * The future can complete successfully or throw an {@link ExecutionException} wrapping
	 * - {@link DecodingFailedException} when the decoding cannot be completed successfully
	 *
	 * @return {@link CompletableFuture}
	 */
	public CompletableFuture<List<JsonObject>> warnings() {
		return this.response.additional().flatMapMany(addl -> {
			return Flux.fromIterable(addl.warnings).map(w -> {
				try {
					return JacksonTransformers.MAPPER.readValue(w, JsonObject.class);
				} catch (IOException ex) {
					throw new DecodingFailedException(ex);
				}
			});
		}).collectList().toFuture();
	}
}