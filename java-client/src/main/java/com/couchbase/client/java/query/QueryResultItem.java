package com.couchbase.client.java.query;

import java.io.IOException;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

public class QueryResultItem {
	private final byte[] encoded;

	public QueryResultItem(byte[] encoded) {
		this.encoded = encoded;
	}

	public JsonObject content() {
		try {
			return JacksonTransformers.MAPPER.readValue(this.encoded, JsonObject.class);
		} catch (IOException ex) {
			throw new CouchbaseException(ex);
		}
	}

	public byte[] getByteValue() {
		return this.encoded;
	}
}
