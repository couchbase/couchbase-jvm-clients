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

public class EventingFunctionUrlBinding {

  private final String hostname;
  private final String alias;
  private final boolean allowCookies;
  private final boolean validateSslCertificates;
  private final EventingFunctionUrlAuth auth;

  public static EventingFunctionUrlBinding create(String hostname, String alias) {
    return builder(hostname, alias).build();
  }

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

  public String hostname() {
    return hostname;
  }

  public String alias() {
    return alias;
  }

  public boolean allowCookies() {
    return allowCookies;
  }

  public boolean validateSslCertificates() {
    return validateSslCertificates;
  }

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

    public Builder allowCookies(boolean allowCookies) {
      this.allowCookies = allowCookies;
      return this;
    }

    public Builder validateSslCertificates(boolean validateSslCertificates) {
      this.validateSslCertificates = validateSslCertificates;
      return this;
    }

    public Builder auth(EventingFunctionUrlAuth auth) {
      this.auth = notNull(auth, "EventingFunctionUrlAuth");
      return this;
    }

    public EventingFunctionUrlBinding build() {
      return new EventingFunctionUrlBinding(this);
    }
  }
}
