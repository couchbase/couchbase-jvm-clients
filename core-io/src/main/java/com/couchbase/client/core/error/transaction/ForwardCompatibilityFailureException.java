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
package com.couchbase.client.core.error.transaction;

import com.couchbase.client.core.error.CouchbaseException;

/**
 * This transaction has been prevented from interacting with documents or metadata owned by another transaction,
 * due to compatibility issues.  The interaction has been prevented in order to prevent data loss or corruption.
 * <p>
 * This error is usually caused by running incompatible transaction clients concurrently, and the solution is to upgrade the
 * older clients.
 */
public class ForwardCompatibilityFailureException extends CouchbaseException {
    public ForwardCompatibilityFailureException() {
        super("This transaction has been prevented from interacting with documents or metadata owned by another transaction " +
                "in order to prevent data loss or corruption.  This error is likely caused by running an older, outdated " +
                "transactions client concurrently with an incompatible newer client - please upgrade.");
    }
}
