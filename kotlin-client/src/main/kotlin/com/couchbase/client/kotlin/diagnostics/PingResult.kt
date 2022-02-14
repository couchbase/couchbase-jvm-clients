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

import com.couchbase.client.core.service.ServiceType

public class PingResult internal constructor(
    private val core: com.couchbase.client.core.diagnostics.PingResult,
) {
    /**
     * Holds the individual health of each endpoint in the report.
     */
    public val endpoints: Map<ServiceType, List<EndpointPingReport>> = core.endpoints()
        .mapValues { (_, v) -> v.map { EndpointPingReport(it) } }

    /**
     * The format version of this report.
     */
    public val version: Int = core.version()

    /**
     * The SDK identifier used.
     */
    public val sdk: String = core.sdk()

    /**
     * The report ID (either user provided or auto generated).
     */
    public val id: String = core.id()

    /**
     * Returns this report in the standard JSON format which is consistent across different SDKs.
     */
    public fun exportToJson(): String = core.exportToJson()

    override fun toString(): String {
        return "PingResult(endpoints=$endpoints, version=$version, sdk='$sdk', id='$id')"
    }
}
