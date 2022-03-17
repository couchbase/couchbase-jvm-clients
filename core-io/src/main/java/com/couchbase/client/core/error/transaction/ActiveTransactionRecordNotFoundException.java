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

public class ActiveTransactionRecordNotFoundException extends CouchbaseException {
    private final String atrId;
    private final String attemptId;

    public ActiveTransactionRecordNotFoundException(String atrId, String attemptId) {
        super("Active Transaction Record " + atrId + " not found");
        this.atrId = atrId;
        this.attemptId = attemptId;
    }

    public String atrId() {
        return atrId;
    }

    public String attemptId() {
        return attemptId;
    }
}
