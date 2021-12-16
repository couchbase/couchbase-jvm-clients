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

package com.couchbase.client.java.manager.analytics.link;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.couchbase.client.core.manager.CoreAnalyticsLinkManager.COUCHBASE_TYPE_NAME;
import static com.couchbase.client.core.manager.CoreAnalyticsLinkManager.S3_TYPE_NAME;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static java.util.Objects.requireNonNull;

public class AnalyticsLinkType {
  private static final ConcurrentMap<String, AnalyticsLinkType> values = new ConcurrentHashMap<>();

  public static final AnalyticsLinkType S3_EXTERNAL = AnalyticsLinkType.of(S3_TYPE_NAME);
  public static final AnalyticsLinkType COUCHBASE_REMOTE = AnalyticsLinkType.of(COUCHBASE_TYPE_NAME);

  private final String wireName;

  private AnalyticsLinkType(String wireName) {
    this.wireName = requireNonNull(wireName);
  }

  /**
   * @param wireName (nullable)
   */
  public static AnalyticsLinkType of(String wireName) {
    return values.computeIfAbsent(defaultIfNull(wireName, "unknown"), AnalyticsLinkType::new);
  }

  @JsonValue
  public String wireName() {
    return wireName;
  }

  @Override
  public String toString() {
    return wireName;
  }
}
