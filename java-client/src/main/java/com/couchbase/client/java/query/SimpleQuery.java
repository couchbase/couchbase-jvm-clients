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

import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

import com.couchbase.client.java.json.JsonValue;

/**
 * Simple query for constructing simple string queries
 *
 * @since 3.0.0
 */
public class SimpleQuery implements Query {

	private final String statement;
	private final boolean prepared;

	private SimpleQuery(final String statement, final boolean prepared) {
		this.statement = prepared? "PREPARE " + statement : statement;
		this.prepared = prepared;
	}

	/**
	 * Create a {@link SimpleQuery} with the given statement
	 *
	 * @param statement the query statement
	 * @return created {@link SimpleQuery}
	 */
	public static SimpleQuery create(final String statement) {
		notNullOrEmpty(statement, "Statement");
		return new SimpleQuery(statement, false);
	}

	/**
	 * Create a {@link SimpleQuery} with the given statement. The query will
	 * be prepared and executed else if the prepared attributes are not found in
	 * the local cache.
	 *
	 * @param statement the query statement
	 * @return created {@link SimpleQuery}
	 */
	public static SimpleQuery createPrepared(final String statement) {
		notNullOrEmpty(statement, "Statement");
		return new SimpleQuery(statement, true);
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
	 * Get if the query is prepared
	 *
	 * @return true if prepared
	 */
	@Override
	public boolean prepared() {
		return this.prepared;
	}

	@Override
	public String toString() {
		return "SimpleQuery{" +
						"statement:" + statement() +
						"prepared:" + prepared() + "}";
	}
}