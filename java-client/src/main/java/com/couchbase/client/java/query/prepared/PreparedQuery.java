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

package com.couchbase.client.java.query.prepared;

import java.util.HashMap;
import java.util.Map;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.json.JsonValue;

/**
 * Constructing an already prepared query
 *
 * @since 3.0.0
 */
@Stability.Volatile
public class PreparedQuery {

	private final JsonObject prepared;

	private PreparedQuery() {
		this.prepared = JsonObject.create();
	}

	private void put(String name, Object value) {
		this.prepared.put(name, value);
	}

	public static PreparedQuery fromJsonObject(JsonObject jsonObject) {
		PreparedQuery preparedQuery = new PreparedQuery();
		preparedQuery.put("encoded_plan", jsonObject.get("encoded_plan"));
		preparedQuery.put("prepared", jsonObject.get("name"));
		return preparedQuery;
	}

	public class Builder {
		private String name;
		private String encodedPlan;
		private Map<String, Object> attributes;

		public void withEncodedPlan(String encodedPlan){
			this.encodedPlan = encodedPlan;
		}

		public void withName(String name) {
			this.name = name;
		}

		public void withAttribute(String name, String value) {
			if (this.attributes == null) {
				this.attributes = new HashMap<>();
			}
			this.attributes.put(name, value);
		}

		public void build() {
			PreparedQuery query = new PreparedQuery();
			if (this.name != null) {
				query.put("prepared", this.name);
			}
			if (this.encodedPlan != null) {
				query.put("encoded_plan", this.encodedPlan);
			}
			if (this.attributes != null) {
				this.attributes.forEach(query::put);
			}
		}
	}

	@Stability.Internal
	public JsonObject getQueryJson(JsonValue params) {
		JsonObject query = this.prepared;
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
		return query;
	}
}
