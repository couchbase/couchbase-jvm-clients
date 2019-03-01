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

package com.couchbase.client.core.error;

/**
 * Exception thrown when the Query stream subscribers
 *  - did not send enough requests to consume the entire chunked response in the stream
 *  - do not consume the request response
 *  - do not send a request before the stream times out and releases the response
 *  further on the socket, so the socket can be used for other requests.
 */
public class QueryStreamException extends CouchbaseException {

    public QueryStreamException() {
        super();
    }

    public QueryStreamException(String message) {
        super(message);
    }

    public QueryStreamException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryStreamException(Throwable cause) {
        super(cause);
    }
}
