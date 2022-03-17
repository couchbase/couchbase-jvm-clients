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
import com.couchbase.client.core.msg.kv.DurabilityLevel;

@Stability.Internal
public class DurabilityLevelUtil {
    private DurabilityLevelUtil() {}

    public static String convertDurabilityLevel(DurabilityLevel dl) {
        switch (dl) {
            case MAJORITY: return "m";
            case MAJORITY_AND_PERSIST_TO_ACTIVE: return "pa";
            case PERSIST_TO_MAJORITY: return "pm";
            default: return "n";
        }
    }

    public static DurabilityLevel convertDurabilityLevel(String dl) {
        switch (dl) {
            case "m": return DurabilityLevel.MAJORITY;
            case "pa": return DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
            case "pm": return DurabilityLevel.PERSIST_TO_MAJORITY;
            case "n": return DurabilityLevel.NONE;
            // If it's an unknown durability level, conservatively default to the safest currently possible
            default: return DurabilityLevel.PERSIST_TO_MAJORITY;
        }
    }

}
