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

public class EventingFunctionUrlBinding {
  private String hostname;
  private String alias;
  private boolean allowCookies;
  private boolean validateSslCertificates;
  private EventingFunctionUrlAuth auth;

  public EventingFunctionUrlBinding(String hostname, String alias) {
    this.hostname = hostname;
    this.alias = alias;
  }

  public String hostname() {
    return hostname;
  }

  public EventingFunctionUrlBinding hostname(String hostname) {
    this.hostname = hostname;
    return this;
  }

  public String alias() {
    return alias;
  }

  public EventingFunctionUrlBinding alias(String alias) {
    this.alias = alias;
    return this;
  }

  public boolean allowCookies() {
    return allowCookies;
  }

  public EventingFunctionUrlBinding allowCookies(boolean allowCookies) {
    this.allowCookies = allowCookies;
    return this;
  }

  public boolean validateSslCertificates() {
    return validateSslCertificates;
  }

  public EventingFunctionUrlBinding validateSslCertificates(boolean validateSslCertificates) {
    this.validateSslCertificates = validateSslCertificates;
    return this;
  }

  public EventingFunctionUrlAuth auth() {
    return auth;
  }

  public EventingFunctionUrlBinding auth(EventingFunctionUrlAuth auth) {
    this.auth = auth;
    return this;
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
}
