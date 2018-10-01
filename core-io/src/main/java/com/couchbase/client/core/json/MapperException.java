package com.couchbase.client.core.json;

import com.couchbase.client.core.error.CouchbaseException;

public class MapperException extends CouchbaseException {

    public MapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
