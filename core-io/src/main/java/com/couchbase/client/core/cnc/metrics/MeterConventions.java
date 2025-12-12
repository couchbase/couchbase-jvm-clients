/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.metrics;

import com.couchbase.client.core.annotation.Stability;

import static java.util.Objects.requireNonNull;

@Stability.Uncommitted
public class MeterConventions {
  private MeterConventions() {
  }

  /**
   * If this is present in the tags then it indicates the unit that the metric
   * should be output in.  The only unit that is currently used is "s" for
   * seconds.
   * <p>
   * Note this not indicate the unit the metric is provided in: that is always
   * nanoseconds/
   */
  public static final String METRIC_TAG_UNITS = "__unit";
  public static final String METRIC_TAG_UNIT_SECONDS = "s";

  public static boolean isTagReserved(String tag) {
    requireNonNull(tag);
    return tag.equals(METRIC_TAG_UNITS);
  }
}
