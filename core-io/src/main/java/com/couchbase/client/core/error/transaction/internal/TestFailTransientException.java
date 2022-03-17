/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.error.transaction.internal;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;

/**
 * Used only in testing: injects a FAIL_TRANSIENT error.
 *
 * E.g. a transient server error that could be recovered with a retry of either the operation or the transaction.
 */
@Stability.Internal
public class TestFailTransientException extends CouchbaseException {

    public TestFailTransientException() {
        this("Injecting a FAIL_TRANSIENT error");
    }

    public TestFailTransientException(String msg) {
        super(msg);
    }
}
