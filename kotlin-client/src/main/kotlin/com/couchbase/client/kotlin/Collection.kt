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

package com.couchbase.client.kotlin

import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder

public class Collection internal constructor(
    public val name: String,
    public val scopeName: String,
    public val bucket: Bucket,
) {
    private val defaultSerializer: JsonSerializer = bucket.env.jsonSerializer
    private val defaultTranscoder: Transcoder = bucket.env.transcoder

}
