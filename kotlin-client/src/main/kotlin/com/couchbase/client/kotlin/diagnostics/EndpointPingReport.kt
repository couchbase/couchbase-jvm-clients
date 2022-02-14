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

import com.couchbase.client.core.diagnostics.PingState
import com.couchbase.client.core.service.ServiceType
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

public class EndpointPingReport internal constructor(
    core: com.couchbase.client.core.diagnostics.EndpointPingReport,
) {
    public val type: ServiceType = core.type()

    public val id: String? = core.id()

    /**
     * The local socket identifier as a string.
     */
    public val local: String? = core.local()

    /**
     * The remote socket identifier as a string.
     */
    public val remote: String? = core.remote()

    /**
     * The state of the individual ping.
     */
    public val state: PingState = core.state()

    /**
     * If present, the namespace of this endpoint.
     */
    public val namespace: String? = core.namespace().orElse(null)

    /**
     * The latency for this ping (might be the timeout property if timed out).
     */
    public val latency: Duration = core.latency().toKotlinDuration()

    /**
     * The error of the ping.
     */
    public val error: String? = core.error().orElse(null)

    override fun toString(): String {
        return "EndpointPingReport(type=$type, id=$id, local=$local, remote=$remote, state=$state, namespace=$namespace, latency=$latency, error=$error)"
    }
}
