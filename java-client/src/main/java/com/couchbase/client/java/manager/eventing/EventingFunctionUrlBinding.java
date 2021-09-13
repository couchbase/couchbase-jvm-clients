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

package com.couchbase.client.java.manager.eventing;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * Represents a URL binding of an eventing function.
 */
public class EventingFunctionUrlBinding {

  private final String hostname;
  private final String alias;
  private final boolean allowCookies;
  private final boolean validateSslCertificates;
  private final EventingFunctionUrlAuth auth;

  /**
   * Creates a URL binding with a hostname and its alias and no authentication.
   * <p>
   * If custom authentication or validation is needed, use the {@link  #builder(String, String)} API instead.
   *
   * @param hostname the hostname of the binding.
   * @param alias the alias of the binding.
   * @return a initialized {@link EventingFunctionUrlBinding}.
   */
  public static EventingFunctionUrlBinding create(String hostname, String alias) {
    return builder(hostname, alias).build();
  }

  /**
   * Allows to customize the {@link EventingFunctionUrlBinding} through a Builder API.
   *
   * @param hostname the hostname of the binding.
   * @param alias the alias of the binding.
   * @return A builder which can be turned into a binding once its properties are configured.
   */
  public static Builder builder(String hostname, String alias) {
    return new Builder(hostname, alias);
  }

  private EventingFunctionUrlBinding(Builder builder) {
    this.hostname = builder.hostname;
    this.alias = builder.alias;
    this.allowCookies = builder.allowCookies;
    this.validateSslCertificates = builder.validateSslCertificates;
    this.auth = builder.auth;
  }

  /**
   * Returns the hostname for the URL binding.
   */
  public String hostname() {
    return hostname;
  }

  /**
   * Returns the alias for the URL binding.
   */
  public String alias() {
    return alias;
  }

  /**
   * Returns true if cookies should be allowed.
   */
  public boolean allowCookies() {
    return allowCookies;
  }

  /**
   * Returns true if TLS certificates should be validated.
   */
  public boolean validateSslCertificates() {
    return validateSslCertificates;
  }

  /**
   * Returns the authentication mechanism configured, including no authentication.
   */
  public EventingFunctionUrlAuth auth() {
    return auth;
  }


  @Override
  public String toString() {
    return "EventingFunctionUrlBinding{" +
      "hostname='" + redactMeta(hostname) + '\'' +
      ", alias='" + redactMeta(alias) + '\'' +
      ", allowCookies=" + allowCookies +
      ", validateSslCertificates=" + validateSslCertificates +
      ", auth=" + auth +
      '}';
  }

  /**
   * Allows configuring properties for the {@link EventingFunctionUrlBinding}.
   */
  public static class Builder {

    private final String hostname;
    private final String alias;

    private boolean allowCookies;
    private boolean validateSslCertificates;
    private EventingFunctionUrlAuth auth = EventingFunctionUrlAuth.noAuth();

    private Builder(String hostname, String alias) {
      this.hostname = notNullOrEmpty(hostname, "Hostname");
      this.alias = notNullOrEmpty(alias, "Alias");
    }

    /**
     * Set to true if cookies should be allowed.
     *
     * @param allowCookies true if cookies should be allowed.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder allowCookies(boolean allowCookies) {
      this.allowCookies = allowCookies;
      return this;
    }

    /**
     * Set to true if SSL/TLS certificates should be validated.
     *
     * @param validateSslCertificates true if certs should be validated.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder validateSslCertificates(boolean validateSslCertificates) {
      this.validateSslCertificates = validateSslCertificates;
      return this;
    }

    /**
     * Allows to configure the URL authentication mechanism that should be used.
     * <p>
     * Please refer to the static builder options available on {@link EventingFunctionUrlAuth} for more
     * information.
     *
     * @param auth the url authentication mechanism that should be used.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder auth(final EventingFunctionUrlAuth auth) {
      this.auth = notNull(auth, "EventingFunctionUrlAuth");
      return this;
    }

    /**
     * Builds the immutable {@link EventingFunctionUrlBinding} for consumption.
     */
    public EventingFunctionUrlBinding build() {
      return new EventingFunctionUrlBinding(this);
    }

  }

}
