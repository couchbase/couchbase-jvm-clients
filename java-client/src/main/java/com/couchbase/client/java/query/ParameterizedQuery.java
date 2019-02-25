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

import static com.couchbase.client.core.util.Validators.*;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * Parameterized Query where the parameters can be positional or named
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class ParameterizedQuery implements Query {

	private final String statement;
	private final JsonValue parameters;
	private final boolean prepared;

	@Stability.Internal
	private ParameterizedQuery(final String statement, final JsonValue parameters, final Boolean prepared) {
		this.statement = prepared ? "PREPARE " + statement : statement;
		this.parameters = parameters;
		this.prepared = prepared;
	}

	/**
	 * Create a positional {@link ParameterizedQuery} with the given statement and positional values
	 *
	 * @param statement the query statement
	 * @param positionalValues the positional values
	 * @return created {@link ParameterizedQuery}
	 */
	public static ParameterizedQuery create(final String statement, final JsonArray positionalValues) {
		notNullOrEmpty(statement, "Statement");
		notNull(positionalValues, "Positional Values");
		return new ParameterizedQuery(statement, positionalValues, false);
	}

	/**
	 * Create a named {@link ParameterizedQuery} with the given statement and named values
	 *
	 * @param statement the query statement
	 * @param namedValues the names and values for the query
	 * @return created {@link ParameterizedQuery}
	 */
	public static ParameterizedQuery create(final String statement, final JsonObject namedValues) {
		notNullOrEmpty(statement, "Statement");
		notNull(namedValues, "Named Values");
		return new ParameterizedQuery(statement, namedValues, false);
	}

	/**
	 * Create a positional {@link ParameterizedQuery} with the given statement and positional values. The query will
	 * be prepared and executed else if the prepared attributes are not found in
	 * the local cache.
	 *
	 * @param statement the query statement
	 * @param positionalValues the positional values
	 * @return created {@link ParameterizedQuery}
	 */
	public static ParameterizedQuery createPrepared(final String statement, final JsonArray positionalValues) {
		notNullOrEmpty(statement, "Statement");
		notNull(positionalValues, "Positional Values");
		return new ParameterizedQuery(statement, positionalValues, true);
	}

	/**
	 * Create a named {@link ParameterizedQuery} with the given statement and named values. The query will
	 * be prepared and executed else if the prepared attributes are not found in
	 * the local cache.
	 *
	 * @param statement the query statement
	 * @param namedValues the names and values for the query
	 * @return created {@link ParameterizedQuery}
	 */
	public static ParameterizedQuery createPrepared(final String statement, final JsonObject namedValues) {
		notNullOrEmpty(statement, "Statement");
		notNull(namedValues, "Named Values");
		return new ParameterizedQuery(statement, namedValues, true);
	}

	/**
	 * Get the query statement string
	 *
	 * @return query statement
	 */
	@Override
	public String statement() {
		return this.statement;
	}

	/**
	 * Get the parameters set for the query
	 *
	 * @return positional or named values
	 */
	@Override
	public JsonValue parameters() {
		return this.parameters;
	}

	/**
	 * Get if the query is prepared or not
	 *
	 * @return true if prepared else false
	 */
	@Override
	public boolean prepared() {
		return this.prepared;
	}

	@Override
	public String toString() {
		return "ParameterizedQuery{" +
						"statement:" + this.statement() +
						"parameters:" + this.parameters().toString() +
						"}";
	}
}