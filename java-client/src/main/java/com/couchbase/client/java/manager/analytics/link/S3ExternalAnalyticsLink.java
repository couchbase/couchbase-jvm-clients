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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty.Access.READ_ONLY;

/**
 * An analytics link to S3.
 * <p>
 * Important: When creating a link to the Amazon S3 service, be sure to follow best practices for security. AWS root
 * account credentials should never be used. The policy for the created IAM User roles should be as strict as
 * possible and only allow access to the required data and required resources. You only need to know the Access
 * Key Id and the Secret Access Key for the created IAM User role to access the S3 service. The link will be
 * able to access whatever is permitted to the IAM User, since it will be using the IAM User credentials to
 * interact with the AWS S3 service.
 */
@SuppressWarnings("unused")
@SinceCouchbase("7.0")
public class S3ExternalAnalyticsLink extends AnalyticsLink {

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

  /**
   * Creates a new Analytics Link to S3.
   * <p>
   * As an alternative to this constructor, {@link AnalyticsLink#s3(String, String)} can be used as well.
   * <p>
   * Please note that additional parameters are required and must be set on {@link S3ExternalAnalyticsLink} in order
   * for a S3 link to work properly.
   */
  public S3ExternalAnalyticsLink(final String name, final String dataverse) {
    super(name, dataverse);
  }

  // For Jackson
  private S3ExternalAnalyticsLink() {
    super("", "");
  }

  @Override
  public AnalyticsLinkType type() {
    return AnalyticsLinkType.S3_EXTERNAL;
  }

  /**
   * Returns the S3 access key ID.
   *
   * @return the S3 access key ID.
   */
  public String accessKeyId() {
    return accessKeyId;
  }

  /**
   * Sets the S3 access key ID (required).
   *
   * @param accessKeyId the S3 access key ID.
   * @return this {@link S3ExternalAnalyticsLink} instance for chaining.
   */
  public S3ExternalAnalyticsLink accessKeyId(final String accessKeyId) {
    this.accessKeyId = accessKeyId;
    return this;
  }

  /**
   * Returns the S3 secret access key.
   *
   * @return the S3 access key.
   */
  public String secretAccessKey() {
    return secretAccessKey;
  }

  /**
   * Sets the S3 secret access key (required).
   *
   * @param secretAccessKey the S3 secret access key.
   * @return this {@link S3ExternalAnalyticsLink} instance for chaining.
   */
  public S3ExternalAnalyticsLink secretAccessKey(final String secretAccessKey) {
    this.secretAccessKey = secretAccessKey;
    return this;
  }

  /**
   * Returns the S3 region.
   *
   * @return the S3 region.
   */
  public String region() {
    return region;
  }

  /**
   * Sets the S3 region (required).
   *
   * @param region the S3 region.
   * @return this {@link S3ExternalAnalyticsLink} instance for chaining.
   */
  public S3ExternalAnalyticsLink region(final String region) {
    this.region = region;
    return this;
  }

  /**
   * Returns the S3 service endpoint.
   *
   * @return the S3 service endpoint.
   */
  public String serviceEndpoint() {
    return serviceEndpoint;
  }

  /**
   * Sets the S3 service endpoint.
   *
   * @param serviceEndpoint the service endpoint.
   * @return this {@link S3ExternalAnalyticsLink} instance for chaining.
   */
  public S3ExternalAnalyticsLink serviceEndpoint(final String serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    return this;
  }

  /**
   * Returns the S3 session token.
   *
   * @return the S3 session token.
   */
  public String sessionToken() {
    return sessionToken;
  }

  /**
   * Sets the S3 session token.
   *
   * @param sessionToken the S3 session token.
   * @return this {@link S3ExternalAnalyticsLink} instance for chaining.
   */
  public S3ExternalAnalyticsLink sessionToken(final String sessionToken) {
    this.sessionToken = sessionToken;
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
