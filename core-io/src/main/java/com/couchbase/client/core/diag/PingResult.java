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
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Contains a report for all the internal service health states.
 *
 * @author Michael Nitschinger
 * @since 1.5.4
 */
@Stability.Volatile
public class PingResult {
    static final int VERSION = 1;

    private final List<PingServiceHealth> services;
    private final String sdk;
    private final String id;
    private final int version;
    private final long configRev;

    public PingResult(List<PingServiceHealth> services, String sdk, String id, long configRev) {
        this.services = services;
        this.version = VERSION;
        this.sdk = sdk;
        this.configRev = configRev;
        this.id = id == null ? UUID.randomUUID().toString() : id;
    }

    public List<PingServiceHealth> services() {
        return services;
    }

    /**
     * Exports this report into the RFC JSON format.
     *
     * @return the as JSON encoded report.
     */
    public String exportToJson() {
        return exportToJson(false);
    }

    /**
     * Exports this report into the RFC JSON format.
     *
     * @return the as JSON encoded report.
     */
    public String exportToJson(boolean pretty) {
        Map<String, Object> result = new HashMap<String, Object>();
        Map<String, List<Map<String, Object>>> services = new HashMap<String, List<Map<String, Object>>>();

        for (PingServiceHealth h : this.services) {
            String type = DiagnosticsResult.serviceTypeFromEnum(h.type());
            if (!services.containsKey(type)) {
                services.put(type, new ArrayList<Map<String, Object>>());
            }
            List<Map<String, Object>> eps = services.get(type);
            eps.add(h.toMap());
        }

        result.put("config_rev", configRev);
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

    public String sdk() {
        return sdk;
    }

    public String id() {
        return id;
    }

    public int version() {
        return version;
    }

    public long configRev() {
        return configRev;
    }

    @Override
    public String toString() {
        return "PingResult{" +
            "services=" + services +
            '}';
    }
}
