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

import com.couchbase.client.core.annotation.SinceCouchbase;
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

/**
 * Represents a Link to an external datasource that is connected with the Analytics service.
 */
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

  /**
   * Static factory method to create an S3 link.
   * <p>
   * Please note that additional parameters are required and must be set on {@link S3ExternalAnalyticsLink} in order
   * for a S3 link to work properly.
   *
   * @param linkName the name of the link.
   * @param dataverseName the dataverse name inside which the link exists.
   * @return the created link instance.
   */
  @SinceCouchbase("7.0")
  public static S3ExternalAnalyticsLink s3(final String linkName, final String dataverseName) {
    return new S3ExternalAnalyticsLink(linkName, dataverseName);
  }

  /**
   * Static factory method to create a remote Couchbase link.
   *
   * @param linkName the name of the link.
   * @param dataverseName the dataverse name inside which the link exists.
   * @return the created link instance.
   */
  public static CouchbaseRemoteAnalyticsLink couchbaseRemote(final String linkName, final String dataverseName) {
    return new CouchbaseRemoteAnalyticsLink(linkName, dataverseName);
  }

  /**
   * Creates a new {@link AnalyticsLink}.
   *
   * @param name the name of the link.
   * @param dataverse the dataverse in which this link is stored.
   */
  protected AnalyticsLink(final String name, final String dataverse) {
    this.name = requireNonNull(name);
    this.dataverse = requireNonNull(dataverse);
  }

  /**
   * Returns the type of the link.
   *
   * @return the type of the link.
   */
  public abstract AnalyticsLinkType type();

  /**
   * Returns the name of the analytics link.
   *
   * @return the name of the link.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the name of the dataverse in which this link is stored.
   *
   * @return the name of the dataverse.
   */
  public String dataverse() {
    return dataverse;
  }

  /**
   * Returns an (internal) representation of this link as a map.
   *
   * @return a map representation of this link.
   */
  @Stability.Internal
  public Map<String, String> toMap() {
    return Mapper.convertValue(this, new TypeReference<Map<String, String>>() {
    });
  }

}


