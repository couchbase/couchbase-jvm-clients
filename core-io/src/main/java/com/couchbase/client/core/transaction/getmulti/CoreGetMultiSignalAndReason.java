/*
 * Copyright 2025 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.getmulti;

import com.couchbase.client.core.annotation.Stability;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreGetMultiSignalAndReason {
    public static final CoreGetMultiSignalAndReason CONTINUE = new CoreGetMultiSignalAndReason(CoreGetMultiSignal.CONTINUE, "Continuing as normal");
    public static final CoreGetMultiSignalAndReason COMPLETED = new CoreGetMultiSignalAndReason(CoreGetMultiSignal.COMPLETED, "Completing as normal");
    public static final CoreGetMultiSignalAndReason BOUND_EXCEEDED = new CoreGetMultiSignalAndReason(CoreGetMultiSignal.BOUND_EXCEEDED, "Operation bound exceeded - aiming to return what we have, if possible");

    public final CoreGetMultiSignal signal;
    public final String reason;

    public CoreGetMultiSignalAndReason(CoreGetMultiSignal signal, String reason) {
        this.signal = requireNonNull(signal);
        this.reason = requireNonNull(reason);
    }
}
