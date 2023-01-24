/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;

/**
 * Query profiling information received from the server query engine.
 *
 * @since 3.0.0
 */
@Stability.Internal
public enum CoreQueryProfile {

  /**
   * No profiling information is added to the query response.
   */
  OFF {
    @Override
    public String toString() {
      return "off";
    }
  },

  /**
   * The query response includes a profile section with stats and details about various phases of the query plan and
   * execution.
   * <p>
   * Three phase times will be included in the system:active_requests and system:completed_requests monitoring
   * keyspaces.
   */
  PHASES {
    @Override
    public String toString() {
      return "phases";
    }
  },

  /**
   * Besides the phase times, the profile section of the query response document will include a full query plan with
   * timing and information about the number of processed documents at each phase.
   * <p>
   * This information will be included in the system:active_requests and system:completed_requests keyspaces.
   */
  TIMINGS {
    @Override
    public String toString() {
      return "timings";
    }
  }
}
