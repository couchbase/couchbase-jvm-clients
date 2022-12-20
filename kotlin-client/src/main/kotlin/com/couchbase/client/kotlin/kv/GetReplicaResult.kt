/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.api.kv.CoreGetResult
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.Transcoder

/**
 * The result of retrieving a full document, possibly from a replica
 * instead of the primary.
 */
public class GetReplicaResult internal constructor(
    id: String,
    r: CoreGetResult,
    defaultTranscoder: Transcoder,
) : GetResult(
    id,
    r.cas(),
    Content(r.content(), r.flags()),
    Expiry.Unknown,
    defaultTranscoder,
) {
    /**
     * True if the result is from a replica; false if it's from the primary.
     */
    public val replica: Boolean = r.replica()

    override fun toString(): String {
        return "GetReplicaResult(replica=$replica, id='$id', cas=$cas, content=$content)"
    }
}

