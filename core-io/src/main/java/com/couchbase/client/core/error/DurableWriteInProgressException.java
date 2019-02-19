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
package com.couchbase.client.core.error;

/**
 * Returned if an attempt is made to mutate a key which already has a durable write pending.
 *
 * @author Graham Pople
 * @since 2.0.0
 */
public class DurableWriteInProgressException extends CouchbaseException implements RetryableOperationException {

    public DurableWriteInProgressException() {
        super();
    }

    public DurableWriteInProgressException(String message) {
        super(message);
    }

    public DurableWriteInProgressException(String message, Throwable cause) {
        super(message, cause);
    }

    public DurableWriteInProgressException(Throwable cause) {
        super(cause);
    }
}
