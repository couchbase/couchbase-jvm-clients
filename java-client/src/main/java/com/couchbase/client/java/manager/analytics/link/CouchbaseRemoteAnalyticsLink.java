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

/**
 * An analytics link to a remote couchbase cluster.
 */
@SuppressWarnings("unused")
public class CouchbaseRemoteAnalyticsLink extends AnalyticsLink {

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

  /**
   * Creates a new Analytics Link to a remote Couchbase cluster.
   * <p>
   * As an alternative to this constructor, {@link AnalyticsLink#couchbaseRemote(String, String)} can be used as well.
   * <p>
   * Please note that additional parameters are required and must be set on {@link CouchbaseRemoteAnalyticsLink} in order
   * for the link to work properly.
   */
  public CouchbaseRemoteAnalyticsLink(final String name, final String dataverse) {
    super(name, dataverse);
  }

  // For Jackson
  private CouchbaseRemoteAnalyticsLink() {
    super("", "");
  }

  @Override
  public AnalyticsLinkType type() {
    return AnalyticsLinkType.COUCHBASE_REMOTE;
  }

  /**
   * Returns the hostname of the remote cluster.
   *
   * @return the hostname of the remote cluster.
   */
  public String hostname() {
    return hostname;
  }

  /**
   * Sets the hostname of the remote cluster (required).
   *
   * @param hostname the hostname of the remote cluster.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink hostname(final String hostname) {
    this.hostname = hostname;
    return this;
  }

  /**
   * Returns the encryption level for the link to the remote cluster.
   *
   * @return the encryption level for the link to the remote cluster.
   */
  public EncryptionLevel encryption() {
    return encryption;
  }

  /**
   * Sets the encryption level for the link to the remote cluster (required).
   *
   * @param encryption the encryption level for the link to the remote cluster.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink encryption(final EncryptionLevel encryption) {
    this.encryption = encryption;
    return this;
  }

  /**
   * Returns the username when connecting to the remote cluster.
   *
   * @return the username when connecting to the remote cluster.
   */
  public String username() {
    return username;
  }

  /**
   * Sets the username when connecting to the remote cluster (required).
   *
   * @param username the username when connecting to the remote cluster.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink username(final String username) {
    this.username = username;
    return this;
  }

  /**
   * Sets the password when connecting to the remote cluster.
   *
   * @return the password when connecting to the remote cluster.
   */
  public String password() {
    return password;
  }

  /**
   * Sets the password when connecting to the remote cluster (required).
   *
   * @param password the password when connecting to the remote cluster.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink password(final String password) {
    this.password = password;
    return this;
  }

  /**
   * Returns the certificate when encryption is used.
   *
   * @return the certificate when encryption is used.
   */
  public String certificate() {
    return certificate;
  }

  /**
   * Sets the certificate when encryption is used.
   *
   * @param certificate the certificate when encryption is used.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink certificate(final String certificate) {
    this.certificate = certificate;
    return this;
  }

  /**
   * Returns the client certificate when encryption is used.
   *
   * @return the client certificate when encryption is used.
   */
  public String clientCertificate() {
    return clientCertificate;
  }

  /**
   * Sets the client certificate when encryption is used.
   *
   * @param clientCertificate the client certificate when encryption is used.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink clientCertificate(final String clientCertificate) {
    this.clientCertificate = clientCertificate;
    return this;
  }

  /**
   * Returns the client key.
   *
   * @return the client key.
   */
  public String clientKey() {
    return clientKey;
  }

  /**
   * Sets the client key.
   *
   * @param clientKey the client key.
   * @return this {@link CouchbaseRemoteAnalyticsLink} for chaining purposes.
   */
  public CouchbaseRemoteAnalyticsLink clientKey(final String clientKey) {
    this.clientKey = clientKey;
    return this;
  }

  @Override
  public String toString() {
    return "CouchbaseRemoteAnalyticsLink{" +
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
