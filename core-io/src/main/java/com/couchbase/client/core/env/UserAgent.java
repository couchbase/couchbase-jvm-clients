/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.env;

import java.util.Optional;

/**
 * Represents a user-agent for this client.
 *
 * @since 2.0.0
 */
public class UserAgent {

  private final String name;
  private final String version;
  private final Optional<String> os;
  private final Optional<String> platform;
  private final String formattedLong;
  private final String formattedShort;

  public UserAgent(final String name, final Optional<String> version, final Optional<String> os, final Optional<String> platform) {
    this.name = name;
    this.version = version.orElse("0.0.0");
    this.os = os;
    this.platform = platform;
    this.formattedLong = "couchbase-" + name + "/" + this.version + formatExtras();
    this.formattedShort = name + "/" + this.version + formatExtras();
  }

  private String formatExtras() {
    if (os.isPresent() && platform.isPresent()) {
      return " (" + os.get() + "; " + platform.get() + ")";
    } else if (os.isPresent()) {
      return " (" + os.get() + ")";
    } else if (platform.isPresent()) {
      return " (" + platform.get() + ")";
    } else {
      return "";
    }
  }

  public String name() {
    return name;
  }

  public String version() {
    return version;
  }

  public Optional<String> os() {
    return os;
  }

  public Optional<String> platform() {
    return platform;
  }

  public String formattedLong() {
    return formattedLong;
  }

  public String formattedShort() {
    return formattedShort;
  }

  @Override
  public String toString() {
    return "UserAgent{" +
      "name='" + name + '\'' +
      ", version='" + version + '\'' +
      ", os=" + os +
      ", platform=" + platform +
      ", formattedLong='" + formattedLong + '\'' +
      ", formattedShort='" + formattedShort + '\'' +
      '}';
  }
}
