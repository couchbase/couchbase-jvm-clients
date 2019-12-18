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
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A diagnostic report for an individual endpoint.
 * <p>
 * Usually this diagnostic information is not looked at in isolation, but rather as part of the overall
 * {@link DiagnosticsResult}.
 */
public class EndpointDiagnostics {

    /**
     * The service type for this endpoint.
     */
    private final ServiceType type;

    /**
     * The current state of the endpoint.
     */
    private final EndpointState state;

    /**
     * The local socket identifier as a string.
     */
    private final String local;

    /**
     * The remote socket identifier as a string.
     */
    private final String remote;

    /**
     * The last activity in microseconds.
     */
    private final Optional<Long> lastActivityUs;

    /**
     * The ID of this endpoint.
     */
    private final String id;

    /**
     * If present, the namespace of this endpoint.
     */
    private final Optional<String> namespace;

    @Stability.Internal
    public EndpointDiagnostics(final ServiceType type, final EndpointState state, final String local,
                               final String remote, final Optional<String> namespace,
                               final Optional<Long> lastActivityUs, final String id) {
        this.type = type;
        this.state = state;
        this.id = id;
        this.local = local;
        this.remote = remote;
        this.lastActivityUs = lastActivityUs;
        this.namespace = namespace;
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
     * If there has been a last activity, returned as a duration.
     */
    public Optional<Duration> lastActivity() {
        return lastActivityUs.map(a -> Duration.ofNanos(TimeUnit.MICROSECONDS.toNanos(a)));
    }

    /**
     * The current state of the endpoint.
     */
    public EndpointState state() {
        return state;
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
        lastActivityUs.ifPresent(a -> map.put("last_activity_us", a));
        map.put("id", id());
        namespace.ifPresent(n -> map.put("namespace", n));
        return map;
    }

    @Override
    public String toString() {
        return "EndpointDiagnostics{" +
          "type=" + type +
          ", state=" + state +
          ", local='" + local + '\'' +
          ", remote='" + remote + '\'' +
          ", lastActivityUs=" + lastActivityUs +
          ", id='" + id + '\'' +
          ", namespace=" + namespace +
          '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndpointDiagnostics that = (EndpointDiagnostics) o;
        return type == that.type &&
          state == that.state &&
          Objects.equals(local, that.local) &&
          Objects.equals(remote, that.remote) &&
          Objects.equals(lastActivityUs, that.lastActivityUs) &&
          Objects.equals(id, that.id) &&
          Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, state, local, remote, lastActivityUs, id, namespace);
    }
}
