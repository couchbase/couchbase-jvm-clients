/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.analytics;

import com.couchbase.client.java.json.JsonObject;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

public class AnalyticsIndex {
  private final String name;
  private final boolean primary;
  private final String datasetName;
  private final String dataverseName;
  private final JsonObject raw;

  public AnalyticsIndex(JsonObject raw) {
    this.raw = requireNonNull(raw);
    this.name = raw.getString("IndexName");
    this.datasetName = raw.getString("DatasetName");
    this.dataverseName = raw.getString("DataverseName");
    this.primary = Boolean.TRUE.equals(raw.getBoolean("IsPrimary"));
  }

  public String name() {
    return name;
  }

  public boolean primary() {
    return primary;
  }

  public String datasetName() {
    return datasetName;
  }

  public String dataverseName() {
    return dataverseName;
  }

  public JsonObject raw() {
    return raw;
  }

  @Override
  public String toString() {
    return redactMeta(raw).toString();
  }
}
