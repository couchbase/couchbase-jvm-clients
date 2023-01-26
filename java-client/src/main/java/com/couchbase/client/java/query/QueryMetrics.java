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

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.ViewServiceException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.Bytes;
import com.couchbase.client.core.util.Golang;
import com.couchbase.client.java.json.JacksonTransformers;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;

/**
 * Query Metrics contains the query result metrics containing counts and timings
 *
 * @since 3.0.0
 */
public abstract class QueryMetrics {

	/**
	 * @return The total time taken for the request, that is the time from when the
	 * request was received until the results were returned, in a human-readable
	 * format (eg. 123.45ms for a little over 123 milliseconds).
	 */
	public abstract Duration elapsedTime();

	/**
	 * @return The time taken for the execution of the request, that is the time from
	 * when query execution started until the results were returned, in a human-readable
	 * format (eg. 123.45ms for a little over 123 milliseconds).
	 */
	public abstract Duration executionTime();

	/**
	 * @return the total number of results selected by the engine before restriction
	 * through LIMIT clause.
	 */
	public abstract long sortCount();

	/**
	 * @return The total number of objects in the results.
	 */
	public abstract long resultCount();

	/**
	 * @return The total number of bytes in the results.
	 */
	public abstract long resultSize();

	/**
	 * @return The number of mutations that were made during the request.
	 */
	public abstract long mutationCount();

	/**
	 * @return The number of errors that occurred during the request.
	 */
	public abstract long errorCount();

	/**
	 * @return The number of warnings that occurred during the request.
	 */
	public abstract long warningCount();
}
