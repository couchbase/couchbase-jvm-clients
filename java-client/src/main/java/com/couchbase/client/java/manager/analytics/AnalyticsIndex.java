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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.json.JsonObject;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

/**
 * Represents an index in analytics.
 */
public class AnalyticsIndex {

  private final String name;
  private final boolean primary;
  private final String datasetName;
  private final String dataverseName;
  private final JsonObject raw;

  /**
   * Creates a new dataset from a raw JSON object.
   *
   * @param raw the decoded JSON object.
   */
  @Stability.Internal
  public AnalyticsIndex(final JsonObject raw) {
    this.raw = requireNonNull(raw);
    this.name = raw.getString("IndexName");
    this.datasetName = raw.getString("DatasetName");
    this.dataverseName = raw.getString("DataverseName");
    this.primary = Boolean.TRUE.equals(raw.getBoolean("IsPrimary"));
  }

  /**
   * Returns the name of the analytics index.
   *
   * @return the name of the analytics index.
   */
  public String name() {
    return name;
  }

  /**
   * Returns true if this index is a primary index.
   *
   * @return true if this index is a primary index.
   */
  public boolean primary() {
    return primary;
  }

  /**
   * Returns the name of the Dataset (collection) this index is part of.
   *
   * @return the name of the Dataset (collection) this index is part of.
   */
  public String datasetName() {
    return datasetName;
  }

  /**
   * Returns the name of the dataverse.
   *
   * @return the name of the dataverse.
   */
  public String dataverseName() {
    return dataverseName;
  }

  /**
   * Returns the "raw" JSON returned from the analytics service.
   *
   * @return the "raw" JSON returned from the analytics service.
   */
  public JsonObject raw() {
    return raw;
  }

  @Override
  public String toString() {
    return "AnalyticsIndex{" +
      "name='" + redactMeta(name) + '\'' +
      ", primary=" + redactMeta(primary) +
      ", datasetName='" + redactMeta(datasetName) + '\'' +
      ", dataverseName='" + redactMeta(dataverseName) + '\'' +
      ", raw=" + redactMeta(raw) +
      '}';
  }

}
