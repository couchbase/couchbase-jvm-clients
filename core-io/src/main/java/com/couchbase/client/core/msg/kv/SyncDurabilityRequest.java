/*
 * Copyright (c) 2019 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;

import java.util.Optional;

/**
 * Marker interface to signal that the given request supports synchronous durability.
 */
public interface SyncDurabilityRequest {

  /**
   * Returns the durability level if present.
   */
  Optional<DurabilityLevel> durabilityLevel();

  /**
   * Helper method to apply the durability level if present to the request span.
   *
   * @param level the level to potentially apply.
   * @param span the span on which it should be applied on.
   */
  default void applyLevelOnSpan(final Optional<DurabilityLevel> level, final RequestSpan span) {
    if (level.isPresent() && span != null) {
      switch (level.get()) {
        case MAJORITY:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "majority");
          break;
        case MAJORITY_AND_PERSIST_TO_ACTIVE:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "majority_and_persist_active");
          break;
        case PERSIST_TO_MAJORITY:
          span.attribute(TracingIdentifiers.ATTR_DURABILITY, "persist_majority");
          break;
      }
    }
  }

}
