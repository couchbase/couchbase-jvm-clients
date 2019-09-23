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
package com.couchbase.client.core.diag;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.service.ServiceType;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;


/**
 * Aggregates the health of one specific {@link Endpoint}.
 *
 * @author Michael Nitschinger
 * @since 1.5.0
 */
@Stability.Volatile
public class EndpointHealth {

    private final ServiceType type;
    private final EndpointState state;
    private final String local;
    private final String remote;
    private final long lastActivityUs;
    private final String id;
    private final Optional<String> scope;

    public EndpointHealth(ServiceType type,
                          EndpointState state,
                          String local,
                          String remote,
                          Optional<String> scope,
                          long lastActivityUs,
                          String id) {
        this.type = type;
        this.state = state;
        this.id = id;
        this.scope = scope;
        this.local = local;
        this.remote = remote;
        this.lastActivityUs = lastActivityUs;
    }

    public ServiceType type() {
        return type;
    }

    public EndpointState state() {
        return state;
    }

    public String local() {
        return local;
    }

    public String remote() {
        return remote;
    }

    public long lastActivity() {
        return lastActivityUs;
    }

    public String id() {
        return id;
    }

    public Optional<String> scope() {
        return scope;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("remote", remote);
        map.put("local", local);
        map.put("state", state().toString().toLowerCase());
        map.put("last_activity_us", lastActivity());
        map.put("id", id());
        scope.ifPresent(sc -> map.put("scope", sc));
        return map;
    }

    @Override
    public String toString() {
        return "EndpointHealth{" +
            "type=" + type +
            ", state=" + state +
            ", local=" + local +
            ", remote=" + remote +
            ", lastActivity=" + lastActivityUs +
            ", scope=" + scope +
            '}';
    }
}
