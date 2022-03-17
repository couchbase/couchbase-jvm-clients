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
package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.annotation.Stability;

/**
 * Metadata related to the CAS.
 *
 * (This was more useful when fallback logic existed for TXNJ-13 that could result in 'stale' non-current CAS, but is
 * left as distinguishing between REAL and LOGICAL modes will help diagnosing any server clock problems.)
 */
@Stability.Internal
public enum CasMode {
    // TXNJ-13: CAS came from ${vbucket.HLC.mode}, and the HLC is currently operating in normal mode.
    REAL,

    // TXNJ-13: CAS came from ${vbucket.HLC.mode}, and the HLC is currently operating in logical mode.  That is, it is
    // waiting for the wallclock to catch up, and is just incrementing by one for each mutation.
    LOGICAL,

    UNKNOWN
}
