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

import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.codec.typeRef

/**
 * The result retrieving a full document.
 */
public class GetResult private constructor(
    /**
     * The document ID of the retrieved document.
     */
    public val id: String,

    /**
     * The Compare And Swap (CAS) value of the document at the time
     * the content was retrieved.
     */
    public val cas: Long,

    /**
     * The retrieved content. Useful for accessing the raw bytes
     * of the document.
     */
    public val content: Content,

    /**
     * A null value means the expiry is unknown because the
     * `withExpiry` argument was `false` when getting the document.
     *
     * If the expiry is known, it will be either [Expiry.None]
     * or [Expiry.Absolute].
     */
    public val expiry: Expiry?,

    @property:PublishedApi
    internal val defaultTranscoder: Transcoder,
) {
    /**
     * Returns the document content after transcoding it into the type
     * specified by the type parameter.
     *
     * @param transcoder the transcoder to use, or null to use the transcoder
     * configured on the cluster environment.
     */
    public inline fun <reified T> contentAs(transcoder: Transcoder? = null): T {
        return (transcoder ?: defaultTranscoder).decode(content, typeRef())
    }

    override fun toString(): String {
        return "GetResult(id='$id', cas=$cas, expiry=$expiry, content=$content)"
    }

    internal companion object {
        fun withKnownExpiry(
            id: String,
            cas: Long,
            content: Content,
            defaultTranscoder: Transcoder,
            expiry: Expiry,
        ) = GetResult(id, cas, content, expiry, defaultTranscoder)

        fun withUnknownExpiry(id: String, cas: Long, content: Content, defaultTranscoder: Transcoder) =
            GetResult(id, cas, content, null, defaultTranscoder)
    }
}
