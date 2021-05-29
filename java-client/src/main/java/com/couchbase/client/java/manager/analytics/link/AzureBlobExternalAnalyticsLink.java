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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty.Access.READ_ONLY;

@Stability.Volatile
@SuppressWarnings("unused")
public class AzureBlobExternalAnalyticsLink extends AnalyticsLink {

  public AzureBlobExternalAnalyticsLink(String name, String dataverse) {
    super(name, dataverse);
  }

  // For Jackson
  private AzureBlobExternalAnalyticsLink() {
    super("", "");
  }

  @JsonProperty(access = READ_ONLY)
  private String connectionString;

  @JsonProperty
  private String accountName;

  @JsonProperty(access = READ_ONLY)
  private String accountKey;

  @JsonProperty(access = READ_ONLY)
  private String sharedAccessSignature;

  @JsonProperty
  private String blobEndpoint;

  @JsonProperty
  private String endpointSuffix;

  public String connectionString() {
    return connectionString;
  }

  public AzureBlobExternalAnalyticsLink connectionString(String connectionString) {
    this.connectionString = connectionString;
    return this;
  }

  public String accountName() {
    return accountName;
  }

  public AzureBlobExternalAnalyticsLink accountName(String accountName) {
    this.accountName = accountName;
    return this;
  }

  public String accountKey() {
    return accountKey;
  }

  public AzureBlobExternalAnalyticsLink accountKey(String accountKey) {
    this.accountKey = accountKey;
    return this;
  }

  public String sharedAccessSignature() {
    return sharedAccessSignature;
  }

  public AzureBlobExternalAnalyticsLink sharedAccessSignature(String sharedAccessSignature) {
    this.sharedAccessSignature = sharedAccessSignature;
    return this;
  }

  public String blobEndpoint() {
    return blobEndpoint;
  }

  public AzureBlobExternalAnalyticsLink blobEndpoint(String blobEndpoint) {
    this.blobEndpoint = blobEndpoint;
    return this;
  }

  public String endpointSuffix() {
    return endpointSuffix;
  }

  public AzureBlobExternalAnalyticsLink endpointSuffix(String endpointSuffix) {
    this.endpointSuffix = endpointSuffix;
    return this;
  }

  @Override
  public AnalyticsLinkType type() {
    return AnalyticsLinkType.AZURE_BLOB_EXTERNAL;
  }

  @Override
  public String toString() {
    return "AzureBlobExternalAnalyticsLink{" +
        "dataverse='" + dataverse() + '\'' +
        ", name='" + name() + '\'' +
        ", hasConnectionString=" + (connectionString != null) +
        ", accountName='" + accountName + '\'' +
        ", hasAccountKey=" + (accountKey != null) +
        ", hasSharedAccessSignature=" + (sharedAccessSignature != null) +
        ", blobEndpoint='" + blobEndpoint + '\'' +
        ", endpointSuffix='" + endpointSuffix + '\'' +
        '}';
  }
}
