/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.error.subdoc;


import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.context.ErrorContext;

/**
 * Subdocument exception thrown when proposed value would make the document too deep to parse.
 *
 * The current limitation is there to ensure a single parse does not consume too much memory (overloading the server).
 * This error is similar to other TooDeep errors, which all relate to various validation stages to ensure the server
 * does not consume too much memory when parsing a single document.
 */
public class ValueTooDeepException extends CouchbaseException {

    public ValueTooDeepException(final ErrorContext ctx) {
        super("Provided value makes the path too deep in the document", ctx);
    }
}
