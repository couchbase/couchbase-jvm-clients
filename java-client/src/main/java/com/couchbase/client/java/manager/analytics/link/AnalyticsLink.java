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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonAlias;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonInclude;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonSubTypes;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.json.Mapper;

import java.util.Map;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.couchbase.client.core.manager.CoreAnalyticsLinkManager.COUCHBASE_TYPE_NAME;
import static com.couchbase.client.core.manager.CoreAnalyticsLinkManager.S3_TYPE_NAME;
import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    visible = true, // so RawExternalAnalyticsLink can view it
    defaultImpl = RawExternalAnalyticsLink.class
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = CouchbaseRemoteAnalyticsLink.class, name = COUCHBASE_TYPE_NAME),
    @JsonSubTypes.Type(value = S3ExternalAnalyticsLink.class, name = S3_TYPE_NAME)
})
@JsonInclude(NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AnalyticsLink {
  @JsonProperty
  private final String name;

  @JsonProperty
  @JsonAlias("scope")
  private final String dataverse;

  public AnalyticsLink(String name, String dataverse) {
    this.name = requireNonNull(name);
    this.dataverse = requireNonNull(dataverse);
  }

  public String name() {
    return name;
  }

  public String dataverse() {
    return dataverse;
  }

  public abstract AnalyticsLinkType type();

  @Stability.Internal
  public Map<String, String> toMap() {
    return Mapper.convertValue(this, new TypeReference<Map<String, String>>() {
    });
  }
}


