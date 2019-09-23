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
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.diag.EndpointHealth;
import com.couchbase.client.core.endpoint.Endpoint;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Aggregates the health of all {@link Endpoint}s.
 *
 * @since 1.5.0
 */
@Stability.Volatile
public class DiagnosticsResult {

    static final int VERSION = 1;

    private final int version;
    private final List<EndpointHealth> endpoints;
    private final String sdk;
    private final String id;

    public DiagnosticsResult(List<EndpointHealth> endpoints, String sdk, String id) {
        this.id = id == null ? UUID.randomUUID().toString() : id;
        this.endpoints = endpoints;
        this.version = VERSION;
        this.sdk = sdk;
    }

    public String id() {
        return id;
    }

    public String sdk() {
        return sdk;
    }

    public List<EndpointHealth> endpoints() {
        return endpoints;
    }

    public List<EndpointHealth> endpoints(final ServiceType type) {
        return endpoints
                .stream()
                .filter(ep -> ep.type().equals(type))
                .collect(Collectors.toList());
    }

    /**
     * Exports this report into the standard JSON format which is consistent
     * across different language SDKs.
     *
     * @return the encoded JSON string.
     */
    public String exportToJson() {
        return exportToJson(false);
    }

    /**
     * Exports this report into the standard JSON format which is consistent
     * across different language SDKs.
     *
     * @return the encoded JSON string.
     */
    public String exportToJson(boolean pretty) {
        Map<String, Object> result = new HashMap<String, Object>();
        Map<String, List<Map<String, Object>>> services = new HashMap<String, List<Map<String, Object>>>();

        for (EndpointHealth h : endpoints()) {
            String type = serviceTypeFromEnum(h.type());
            if (!services.containsKey(type)) {
                services.put(type, new ArrayList<Map<String, Object>>());
            }
            List<Map<String, Object>> eps = services.get(type);
            eps.add(h.toMap());
        }

        result.put("version", version);
        result.put("services", services);
        result.put("sdk", sdk);
        result.put("id", id);

        try {
            if (pretty) {
                return Mapper.writer().withDefaultPrettyPrinter().writeValueAsString(result);
            } else {
                return Mapper.writer().writeValueAsString(result);
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not encode as JSON string.", e);
        }
    }

    static String serviceTypeFromEnum(ServiceType type) {
        switch(type) {
            case VIEWS:
                return "view";
            case KV:
                return "kv";
            case QUERY:
                return "n1ql";
            case MANAGER:
                return "mgmt";
            case SEARCH:
                return "fts";
            case ANALYTICS:
                return "cbas";
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public String toString() {
        return "ServicesHealth{" +
            "version=" + version +
            ", endpoints=" + endpoints +
            '}';
    }
}
