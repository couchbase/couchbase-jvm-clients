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

package com.couchbase.client.kotlin.query

import com.couchbase.client.core.api.query.CoreQueryProfile
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi


/**
 * Controls how much query profiling information is included in the response
 * metadata.
 *
 * Access the returned info via [QueryMetadata.profile].
 */
@VolatileCouchbaseApi
public enum class QueryProfile(internal val core: CoreQueryProfile) {

    /**
     * No profiling information is added to the query response.
     */
    OFF(CoreQueryProfile.OFF),

    /**
     * The query response includes a profile section with stats and details
     * about various phases of the query plan and execution.
     *
     * Three phase times will be included in the system:active_requests and
     * system:completed_requests monitoring keyspaces.
     */
    PHASES(CoreQueryProfile.PHASES),

    /**
     * Besides the phase times, the profile section of the query response
     * document will include a full query plan with timing and information
     * about the number of processed documents at each phase.
     *
     * This information will be included in the system:active_requests and
     * system:completed_requests keyspaces.
     */
    TIMINGS(CoreQueryProfile.TIMINGS),
    ;

    override fun toString(): String = core.toString()
}
