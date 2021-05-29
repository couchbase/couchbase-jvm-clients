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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonAlias;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;

import static com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty.Access.READ_ONLY;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unused")
public class CouchbaseRemoteAnalyticsLink extends AnalyticsLink {

  public CouchbaseRemoteAnalyticsLink(String name, String dataverse) {
    super(name, dataverse);
  }

  // For Jackson
  private CouchbaseRemoteAnalyticsLink() {
    super("", "");
  }

  @JsonProperty
  @JsonAlias("activeHostname")
  private String hostname;

  @JsonProperty
  private EncryptionLevel encryption;

  @JsonProperty
  private String username;

  @JsonProperty(access = READ_ONLY)
  private String password;

  @JsonProperty
  private String certificate;

  @JsonProperty
  private String clientCertificate;

  @JsonProperty(access = READ_ONLY)
  private String clientKey;

  public String hostname() {
    return hostname;
  }

  public CouchbaseRemoteAnalyticsLink hostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  public EncryptionLevel encryption() {
    return encryption;
  }

  public CouchbaseRemoteAnalyticsLink encryption(EncryptionLevel encryption) {
    this.encryption = encryption;
    return this;
  }

  public String username() {
    return username;
  }

  public CouchbaseRemoteAnalyticsLink username(String username) {
    this.username = username;
    return this;
  }

  public String password() {
    return password;
  }

  public CouchbaseRemoteAnalyticsLink password(String password) {
    this.password = password;
    return this;
  }

  public String certificate() {
    return certificate;
  }

  public CouchbaseRemoteAnalyticsLink certificate(String certificate) {
    this.certificate = certificate;
    return this;
  }

  public String clientCertificate() {
    return clientCertificate;
  }

  public CouchbaseRemoteAnalyticsLink clientCertificate(String clientCertificate) {
    this.clientCertificate = clientCertificate;
    return this;
  }

  public String clientKey() {
    return clientKey;
  }

  public CouchbaseRemoteAnalyticsLink clientKey(String clientKey) {
    this.clientKey = clientKey;
    return this;
  }

  @Override
  public AnalyticsLinkType type() {
    return AnalyticsLinkType.COUCHBASE_REMOTE;
  }

  @Override
  public String toString() {
    return "CouchbaseAnalyticsLink{" +
        "encryption=" + encryption +
        ", hostname='" + hostname + '\'' +
        ", username='" + username + '\'' +
        ", hasPassword=" + (password != null) +
        ", certificate='" + certificate + '\'' +
        ", clientCertificate='" + clientCertificate + '\'' +
        ", hasClientKey=" + (clientKey != null) +
        '}';
  }

  /**
   * Security options for remote Couchbase links.
   */
  public enum EncryptionLevel {
    /**
     * Connect to the remote Couchbase cluster using an unsecured channel.
     * Send the password in plaintext.
     */
    NONE("none"),

    /**
     * Connect to the remote Couchbase cluster using an unsecured channel.
     * Send the password securely using SASL.
     */
    HALF("half"),

    /**
     * Connect to the remote Couchbase cluster using a channel secured by TLS.
     * If a password is used, it is sent over the secure channel.
     * <p>
     * Requires specifying the certificate to trust.
     *
     * @see CouchbaseRemoteAnalyticsLink#certificate(String)
     */
    FULL("full"),
    ;

    private final String wireName;

    EncryptionLevel(String wireName) {
      this.wireName = requireNonNull(wireName);
    }

    @JsonValue
    public String wireName() {
      return wireName;
    }
  }
}
