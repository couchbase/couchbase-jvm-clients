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
 * Subdocument exception thrown when existing number value in document is too big.
 *
 * The value is interpreted as 64 bit on the server side so the acceptable range
 * is that of Java's long ({@link Long#MIN_VALUE} to {@link Long#MAX_VALUE}).
 */
public class NumberTooBigException extends CouchbaseException {

    public NumberTooBigException(final ErrorContext ctx) {
        super("Stored numeric value is too big", ctx);
    }
}
