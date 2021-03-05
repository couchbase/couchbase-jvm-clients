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
 * An API marked as internal is not part of the SDK's public API, and may not
 * be relied upon at all.
 *
 * These interfaces are not intended for external consumption. They are
 * visible only because they can't be made private due to technical limitations
 * (usually related to inlining).
 *
 * Use at your own risk!
 *
 * Note: This annotation corresponds to [Stability.Internal] from the
 * Couchbase Java SDK and core-io library. The Kotlin SDK uses a separate
 * annotation in order to specify opt-in requirements.
 */
@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
public annotation class InternalApi

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
@RequiresOptIn(level = RequiresOptIn.Level.ERROR)
public annotation class VolatileApi

/**
 * No commitment is made about the API.
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
@RequiresOptIn(level = RequiresOptIn.Level.WARNING)
public annotation class UncommittedApi
