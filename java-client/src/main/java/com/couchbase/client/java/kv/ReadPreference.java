/*
 * Copyright (c) 2024 Couchbase, Inc.
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
package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.core.error.InvalidArgumentException;

/**
 * Represents the read preference for a given replica get operation.
 */
public enum ReadPreference {
  /**
   * No preference is set.
   */
  NO_PREFERENCE,

    /**
     * This operation will aim to read from a preferred server group,
   * that is configured at Cluster initialization time.
   */
  PREFERRED_SERVER_GROUP;

  @Stability.Internal
  CoreReadPreference toCore() {
    switch (this) {
      case NO_PREFERENCE:
        return CoreReadPreference.NO_PREFERENCE;
      case PREFERRED_SERVER_GROUP:
        return CoreReadPreference.PREFERRED_SERVER_GROUP;
      default:
        throw InvalidArgumentException.fromMessage("Unexpected ReadPreference value: " + this);
    }
  }
}
