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
 * Concurrent Key-Value operations on the same document have been detected.
 *
 * This is not permitted.  Most such operations will fail - for instance, it doesn't make
 * conceptual sense to be simultaneously inserting and deleting the same document.  At best,
 * it results in an ambiguous race as to which succeeds.  Hence, this situation is regarded
 * as an application bug that needs to be resolved, and will fail the transaction.
 *
 * Note that this situation cannot always be detected.  E.g. if two get-and-replaces are performed concurrently, they may happen
 * to perform in serial (as it's a race) in which case they will both succeed.
 */
public class ConcurrentOperationsDetectedOnSameDocumentException extends CouchbaseException {
    public ConcurrentOperationsDetectedOnSameDocumentException() {
        super("Concurrent Key-Value operations on the same document have been detected.  This is an application error that must be resolved.");
    }
}
