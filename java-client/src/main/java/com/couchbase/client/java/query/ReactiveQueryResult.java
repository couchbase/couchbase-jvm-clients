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

package com.couchbase.client.java.query;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveQueryResult {

	private final Mono<QueryResponse> response;

	public ReactiveQueryResult(CompletableFuture<QueryResponse> response) {
		this.response = Mono.fromFuture(response);
	}

	public Mono<String> requestId() {
		return this.response.flatMap(QueryResponse::requestId);
	}

	public Mono<String> clientContextId() {
		return this.response.flatMap(QueryResponse::clientContextId);
	}

	public Mono<QueryMetrics> info() {
		return this.response.flatMap(r -> r.metrics().map(n -> {
			try {
				JsonObject jsonObject = JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
				return new QueryMetrics(jsonObject);
			} catch (IOException ex) {
				throw new CouchbaseException(ex);
			}
		}));
	}

	public Mono<String> queryStatus() {
		return this.response.flatMap(QueryResponse::clientContextId);
	}

	public Flux<JsonObject> rows() {
		return this.response.flux().flatMap(r -> r.rows().map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
			} catch (IOException ex) {
				throw new CouchbaseException(ex);
			}
		}));
	}

	public <T>Flux<T> rows(Class<T> target, Decoder<T> decoder) {
		return this.response.flux().flatMap(r -> r.rows().map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n, target);
			} catch (IOException ex) {
				throw new CouchbaseException(ex);
			}
		}));
	}

	public Flux<JsonObject> warnings() {
		return this.response.flux().flatMap(r -> r.warnings().map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
			} catch (IOException ex) {
				throw new CouchbaseException(ex);
			}
		}));
	}

	public Flux<JsonObject> errors() {
		return this.response.flux().flatMap(r -> r.errors().map(n -> {
			try {
				return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
			} catch (IOException ex) {
				throw new IllegalStateException(ex);
			}
		}));
	}
}
