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

package com.couchbase.client.core.transaction.support;

import com.couchbase.client.core.annotation.Stability;

@Stability.Internal
public class TransactionFields {
    private TransactionFields() {}

    // Fields in the Active Transaction Records
    // These are keep as brief as possible, more important to reduce changes of doc overflowing
    // than to preserve human debuggability
    public final static String ATR_FIELD_ATTEMPTS = "attempts";
    public final static String ATR_FIELD_TRANSACTION_ID = "tid";
    public final static String ATR_FIELD_STATUS = "st";
    public final static String ATR_FIELD_START_TIMESTAMP = "tst";
    public final static String ATR_FIELD_EXPIRES_AFTER_MILLIS = "exp";
    public final static String ATR_FIELD_FORWARD_COMPATIBILITY = "fc";
    public final static String ATR_FIELD_START_COMMIT = "tsc";
    public final static String ATR_FIELD_COMMIT_ONLY_IF_NOT_ABORTED = "p";
    public final static String ATR_FIELD_TIMESTAMP_COMPLETE = "tsco";
    public final static String ATR_FIELD_TIMESTAMP_ROLLBACK_START = "tsrs";
    public final static String ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE = "tsrc";
    public final static String ATR_FIELD_DOCS_INSERTED = "ins";
    public final static String ATR_FIELD_DOCS_REPLACED = "rep";
    public final static String ATR_FIELD_DOCS_REMOVED = "rem";
    public final static String ATR_FIELD_DURABILITY_LEVEL = "d";
    public final static String ATR_FIELD_PER_DOC_ID = "id";
    public final static String ATR_FIELD_PER_DOC_BUCKET = "bkt";
    public final static String ATR_FIELD_PER_DOC_SCOPE = "scp";
    public final static String ATR_FIELD_PER_DOC_COLLECTION = "col";

    // Fields inside regular docs that are part of a transaction
    public final static String TRANSACTION_INTERFACE_PREFIX_ONLY = "txn";
    public final static String TRANSACTION_INTERFACE_PREFIX = TRANSACTION_INTERFACE_PREFIX_ONLY + ".";
    public final static String TRANSACTION_RESTORE_PREFIX_ONLY = TRANSACTION_INTERFACE_PREFIX_ONLY + ".restore";
    public final static String TRANSACTION_RESTORE_PREFIX = TRANSACTION_RESTORE_PREFIX_ONLY + ".";
    public final static String TRANSACTION_ID = TRANSACTION_INTERFACE_PREFIX + "id.txn";
    public final static String ATTEMPT_ID = TRANSACTION_INTERFACE_PREFIX + "id.atmpt";
    public final static String ATR_ID = TRANSACTION_INTERFACE_PREFIX + "atr.id";
    public final static String ATR_BUCKET_NAME = TRANSACTION_INTERFACE_PREFIX + "atr.bkt";
    // Added in protocol 2
    public final static String ATR_SCOPE_NAME = TRANSACTION_INTERFACE_PREFIX + "atr.scp";
    // This field contains in protocol:
    // 1: "scope_name.collection_name"
    // 2: Just collection name - scope is in ATR_SCOPE_NAME
    public final static String ATR_COLL_NAME = TRANSACTION_INTERFACE_PREFIX + "atr.coll";
    public final static String STAGED_DATA_JSON = TRANSACTION_INTERFACE_PREFIX + "op.stgd";
    public final static String STAGED_DATA_BINARY = TRANSACTION_INTERFACE_PREFIX + "op.bin";
    public final static String OP = TRANSACTION_INTERFACE_PREFIX + "op.type";
}

