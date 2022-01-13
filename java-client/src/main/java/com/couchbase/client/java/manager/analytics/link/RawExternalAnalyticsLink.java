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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

/**
 * A link of unrecognized type. Read-only.
 */
@Stability.Volatile
public class RawExternalAnalyticsLink extends AnalyticsLink {

  private final AnalyticsLinkType type;
  private final String json;

  @JsonCreator
  public static RawExternalAnalyticsLink fromJson(final ObjectNode json) {
    return new RawExternalAnalyticsLink(json);
  }

  private RawExternalAnalyticsLink(final ObjectNode json) {
    super(
      json.path("name").asText(),
      Optional.ofNullable(json.path("dataverse").textValue()).orElse(json.path("scope").asText())
    );
    this.json = json.toString();
    this.type = AnalyticsLinkType.of(json.path("type").textValue());
  }

  @Override
  public Map<String, String> toMap() {
    throw new UnsupportedOperationException("This version of the Couchbase SDK does not know how to update " +
      "analytics links of type '" + type + "'.");
  }

  @Override
  public AnalyticsLinkType type() {
    return type;
  }

  /**
   * Returns the raw JSON representation of the link.
   */
  public String json() {
    return json;
  }

  @Override
  public String toString() {
    // The JSON from the server *probably* doesn't have sensitive values,
    // but let's err on the side of caution and exclude it.
    return "RawExternalAnalyticsLink{" +
        "type=" + type() +
        ", dataverse='" + redactMeta(dataverse()) + '\'' +
        ", name='" + redactMeta(name()) + '\'' +
        '}';
  }
}
