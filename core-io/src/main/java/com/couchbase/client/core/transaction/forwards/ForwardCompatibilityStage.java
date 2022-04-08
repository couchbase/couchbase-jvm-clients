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
package com.couchbase.client.core.transaction.forwards;

import com.couchbase.client.core.annotation.Stability;

import java.util.EnumSet;
import java.util.Objects;

/**
 * Points in the protocol where forward compatibility can be checked.
 */
@Stability.Internal
public enum ForwardCompatibilityStage {
    WRITE_WRITE_CONFLICT_READING_ATR("WW_R"),
    WRITE_WRITE_CONFLICT_REPLACING("WW_RP"),
    WRITE_WRITE_CONFLICT_REMOVING("WW_RM"),
    WRITE_WRITE_CONFLICT_INSERTING("WW_I"),
    WRITE_WRITE_CONFLICT_INSERTING_GET("WW_IG"),
    GETS("G"),
    GETS_READING_ATR("G_A"),
    CLEANUP_ENTRY("CL_E"),
    CAS_MISMATCH_DURING_COMMIT("CM_C"),
    CAS_MISMATCH_DURING_ROLLBACK("CM_R"),
    CAS_MISMATCH_DURING_STAGING("CM_S");

    private final String value;

    ForwardCompatibilityStage(String value) {
        this.value = Objects.requireNonNull(value);
    }

    public String value() {
        return value;
    }
}
