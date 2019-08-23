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

import static java.util.Objects.requireNonNull;

public class AnalyticsDataset {
  private final String name;
  private final String dataverseName;
  private final JsonObject json;

  public AnalyticsDataset(JsonObject json) {
    this.json = requireNonNull(json);
    this.name = requireNonNull(json.getString("DatasetName"));
    this.dataverseName = requireNonNull(json.getString("DataverseName"));
  }

  public String name() {
    return name;
  }

  public String dataverseName() {
    return dataverseName;
  }

  public JsonObject json() {
    return json;
  }

  @Override
  public String toString() {
    return "AnalyticsDataset{" +
        "name='" + name + '\'' +
        ", dataverseName='" + dataverseName + '\'' +
        ", json=" + json +
        '}';
  }
}
