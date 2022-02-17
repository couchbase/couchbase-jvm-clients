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

package com.couchbase.client.kotlin.annotations

import com.couchbase.client.core.annotation.Stability

/**
 * APIs marked as volatile can change any time and for any reason.
 *
 * They may be volatile for reasons including:
 *
 * * Depends on specific implementation detail within the library which
 *   may change in the response.
 *
 * * Depends on specific implementation detail within the server which may
 *   change in the response.
 *
 * * Has been introduced as part of a trial phase for the specific feature.
 *
 * Note: This annotation corresponds to [Stability.Volatile] from the
 * Couchbase Java SDK and core-io library. The Kotlin SDK uses a separate
 * annotation in order to specify opt-in requirements.
 */
@MustBeDocumented
public annotation class VolatileCouchbaseApi

/**
 * The API will probably remain stable, but no commitment has been made yet.
 *
 * It may be changed in incompatible ways or dropped from one release
 * to another. The difference between uncommitted and volatile
 * is that an uncommitted API is more mature and is less likely
 * to be changed.
 *
 * Uncommitted APIs may mature into committed APIs.
 *
 * Note: This annotation corresponds to [Stability.Uncommitted] from the
 * Couchbase Java SDK and core-io library. The Kotlin SDK uses a separate
 * annotation in order to specify opt-in requirements.
 */
@MustBeDocumented
public annotation class UncommittedCouchbaseApi
