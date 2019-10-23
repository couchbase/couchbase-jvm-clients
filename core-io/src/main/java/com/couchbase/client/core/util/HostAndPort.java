/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * Typed tuple that holds an unresolved hostname and port tuple and provides utility access methods.
 */
public class HostAndPort {

  private final String hostname;
  private final int port;

  public HostAndPort(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public String hostname() {
    return hostname;
  }

  public int port() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostAndPort that = (HostAndPort) o;
    return port == that.port &&
      Objects.equals(hostname, that.hostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostname, port);
  }

  @Override
  public String toString() {
    return redactSystem(hostname + ":" + port).toString();
  }
}
