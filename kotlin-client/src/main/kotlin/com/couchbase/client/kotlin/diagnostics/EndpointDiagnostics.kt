/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.diagnostics

import com.couchbase.client.core.endpoint.EndpointState
import com.couchbase.client.core.service.ServiceType
import kotlin.time.toKotlinDuration

public class EndpointDiagnostics internal constructor(
    core: com.couchbase.client.core.diagnostics.EndpointDiagnostics,
) {
    /**
     * The service type for this endpoint.
     */
    public val type: ServiceType = core.type()

    /**
     * The current state of the endpoint.
     */
    public val state: EndpointState = core.state()

    /**
     * The local socket identifier as a string.
     */
    public val local: String? = core.local()

    /**
     * The remote socket identifier as a string.
     */
    public val remote: String? = core.remote()

    /**
     * The namespace of this endpoint (likely the bucket name if present)
     */
    public val namespace: String? = core.namespace().orElse(null)

    /**
     * The last activity.
     */
    public val lastActivity: kotlin.time.Duration? = core.lastActivity()
        .map { it.toKotlinDuration() }
        .orElse(null)

    /**
     * The ID of this endpoint.
     */
    public val id: String? = core.id().orElse(null)

    override fun toString(): String {
        return "EndpointDiagnostics(type=$type, state=$state, local=$local, remote=$remote, namespace=$namespace, lastActivity=$lastActivity, id=$id)"
    }
}
