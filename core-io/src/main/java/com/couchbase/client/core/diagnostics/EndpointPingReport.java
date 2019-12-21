/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class EndpointPingReport {

  /**
   * The service type for this endpoint.
   */
  private final ServiceType type;

  /**
   * The ID of this endpoint.
   */
  private final String id;


  /**
   * The local socket identifier as a string.
   */
  private final String local;

  /**
   * The remote socket identifier as a string.
   */
  private final String remote;

  /**
   * The state of the individual ping.
   */
  private final PingState state;

  /**
   * If present, the namespace of this endpoint.
   */
  private final Optional<String> namespace;

  /**
   * The latency for this ping (might be the timeout property if timed out).
   */
  private final Duration latency;

  /**
   * The error of the ping if needed.
   */
  private final Optional<String> error;

  @Stability.Internal
  public EndpointPingReport(final ServiceType type, final String id, final String local, final String remote,
                            final PingState state, final Optional<String> namespace, final Duration latency,
                            final Optional<String> error) {
    this.type = type;
    this.id = id;
    this.local = local;
    this.remote = remote;
    this.state = state;
    this.namespace = namespace;
    this.latency = latency;
    this.error = error;
  }

  /**
   * The service type for this endpoint.
   */
  public ServiceType type() {
    return type;
  }

  /**
   * The ID for this endpoint.
   */
  public String id() {
    return id;
  }

  /**
   * The local socket address for this endpoint.
   */
  public String local() {
    return local;
  }

  /**
   * The remote socket address for this endpoint.
   */
  public String remote() {
    return remote;
  }

  /**
   * The state of this ping when assembling the report.
   */
  public PingState state() {
    return state;
  }

  /**
   * The latency of this ping.
   */
  public Duration latency() {
    return latency;
  }

  /**
   * The reason this ping did not succeed.
   */
  public Optional<String> error() {
    return error;
  }

  /**
   * The namespace of this endpoint (likely the bucket name if present).
   */
  public Optional<String> namespace() {
    return namespace;
  }

  Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>();
    if (remote != null) {
      map.put("remote", remote);
    }
    if (local != null) {
      map.put("local", local);
    }
    map.put("state", state().toString().toLowerCase());
    if (id != null) {
      map.put("id", id);
    }
    map.put("latency_us", TimeUnit.NANOSECONDS.toMicros(latency.toNanos()));
    namespace.ifPresent(n -> map.put("namespace", n));
    return map;
  }

  @Override
  public String toString() {
    return "EndpointPingReport{" +
      "type=" + type +
      ", id='" + id + '\'' +
      ", local='" + local + '\'' +
      ", remote='" + remote + '\'' +
      ", state=" + state +
      ", namespace=" + namespace +
      ", latency=" + latency +
      ", error=" + error +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EndpointPingReport that = (EndpointPingReport) o;
    return type == that.type &&
      Objects.equals(id, that.id) &&
      Objects.equals(local, that.local) &&
      Objects.equals(remote, that.remote) &&
      state == that.state &&
      Objects.equals(namespace, that.namespace) &&
      Objects.equals(latency, that.latency) &&
      Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id, local, remote, state, namespace, latency, error);
  }
}
