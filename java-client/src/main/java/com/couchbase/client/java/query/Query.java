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
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;
import io.netty.util.CharsetUtil;

/**
 * Query interface for constructing a N1QL Query
 *
 * @since 3.0.0
 */
@Stability.Volatile
public interface Query {

	/**
	 * Query statement string
	 *
	 * @return query string
	 */
	String statement();

	/**
	 * Query positional or named parameters
	 *
	 * @return {@link JsonArray} for positional or {@link JsonValue} for named
	 */
	JsonValue parameters();

	/**
	 * Get if the query is prepared or not
	 *
	 * @return true if prepared else false
	 */
	default boolean prepared() { return false; }

	/**
	 * Encode the query, useful for constructing request
	 *
	 * @return encoded byte array
	 */
	@Stability.Internal
	default byte[] encode() {
		JsonObject query = JsonObject.create().put("statement", this.statement());
		JsonValue params = parameters();
		if (params != null) {
			if (params instanceof JsonArray && !((JsonArray) params).isEmpty()) {
				query.put("args", (JsonArray) params);
			} else if (params instanceof JsonObject && !((JsonObject) params).isEmpty()) {
				JsonObject namedParams = (JsonObject) params;
				namedParams.getNames().forEach(key -> {
					Object value = namedParams.get(key);
					if (key.charAt(0) != '$') {
						query.put('$' + key, value);
					} else {
						query.put(key, value);
					}
				});
			}
		}
		return query.toString().getBytes(CharsetUtil.UTF_8);
	}
}