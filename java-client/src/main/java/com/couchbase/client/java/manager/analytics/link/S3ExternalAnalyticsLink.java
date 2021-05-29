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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty.Access.READ_ONLY;

@SuppressWarnings("unused")
public class S3ExternalAnalyticsLink extends AnalyticsLink {

  public S3ExternalAnalyticsLink(String name, String dataverse) {
    super(name, dataverse);
  }

  // For Jackson
  private S3ExternalAnalyticsLink() {
    super("", "");
  }

  @JsonProperty
  private String accessKeyId;

  @JsonProperty(access = READ_ONLY)
  private String secretAccessKey;

  @JsonProperty(access = READ_ONLY)
  private String sessionToken;

  @JsonProperty
  private String region;

  @JsonProperty
  private String serviceEndpoint;

  @Override
  public AnalyticsLinkType type() {
    return AnalyticsLinkType.S3_EXTERNAL;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public S3ExternalAnalyticsLink accessKeyId(String accessKeyId) {
    this.accessKeyId = accessKeyId;
    return this;
  }

  public String secretAccessKey() {
    return secretAccessKey;
  }

  public S3ExternalAnalyticsLink secretAccessKey(String secretAccessKey) {
    this.secretAccessKey = secretAccessKey;
    return this;
  }

  public String sessionToken() {
    return sessionToken;
  }

  public S3ExternalAnalyticsLink sessionToken(String sessionToken) {
    this.sessionToken = sessionToken;
    return this;
  }

  public String region() {
    return region;
  }

  public S3ExternalAnalyticsLink region(String region) {
    this.region = region;
    return this;
  }

  public String serviceEndpoint() {
    return serviceEndpoint;
  }

  public S3ExternalAnalyticsLink serviceEndpoint(String serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    return this;
  }

  @Override
  public String toString() {
    return "S3ExternalAnalyticsLink{" +
        "dataverse='" + dataverse() + '\'' +
        ", name='" + name() + '\'' +
        ", accessKeyId='" + accessKeyId + '\'' +
        ", hasSecretAccessKey=" + (secretAccessKey != null) +
        ", hasSessionToken=" + (sessionToken != null) +
        ", region='" + region + '\'' +
        ", serviceEndpoint='" + serviceEndpoint + '\'' +
        '}';
  }

}
