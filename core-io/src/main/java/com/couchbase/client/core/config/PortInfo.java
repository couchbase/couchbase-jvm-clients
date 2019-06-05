/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PortInfo {

    private final Map<ServiceType, Integer> ports;
    private final Map<ServiceType, Integer> sslPorts;
    private final Map<String, AlternateAddress> alternateAddresses;
    private final String hostname;

    /**
     * Creates a new {@link PortInfo}.
     *
     * <p>Note that if the hostname is null (not provided by the server), it is explicitly set to
     * null because otherwise the loaded InetAddress would point to localhost.</p>
     *
     * @param services the list of services mapping to ports.
     */
    @JsonCreator
    public PortInfo(
        @JsonProperty("services") Map<String, Integer> services,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("alternateAddresses") Map<String, AlternateAddress> aa
    ) {
        ports = new HashMap<>();
        sslPorts = new HashMap<>();
        alternateAddresses = aa == null ? Collections.emptyMap() : aa;
        try {
            this.hostname = hostname;
        } catch (Exception e) {
            throw new CouchbaseException("Could not analyze hostname from config.", e);
        }

        extractPorts(services, ports, sslPorts);
    }

    /**
     * Helper method to extract ports from the raw services port mapping.
     *
     * @param input the raw input ports
     * @param ports the output direct ports
     * @param sslPorts the output ssl ports
     */
    static void extractPorts(final Map<String, Integer> input,
                             final Map<ServiceType, Integer> ports,
                             final Map<ServiceType, Integer> sslPorts) {
        for (Map.Entry<String, Integer> entry : input.entrySet()) {
            String service = entry.getKey();
            int port = entry.getValue();
          switch (service) {
            case "mgmt":
              ports.put(ServiceType.MANAGER, port);
              break;
            case "capi":
              ports.put(ServiceType.VIEWS, port);
              break;
            case "kv":
              ports.put(ServiceType.KV, port);
              break;
            case "kvSSL":
              sslPorts.put(ServiceType.KV, port);
              break;
            case "capiSSL":
              sslPorts.put(ServiceType.VIEWS, port);
              break;
            case "mgmtSSL":
              sslPorts.put(ServiceType.MANAGER, port);
              break;
            case "n1ql":
              ports.put(ServiceType.QUERY, port);
              break;
            case "n1qlSSL":
              sslPorts.put(ServiceType.QUERY, port);
              break;
            case "fts":
              ports.put(ServiceType.SEARCH, port);
              break;
            case "ftsSSL":
              sslPorts.put(ServiceType.SEARCH, port);
              break;
            case "cbas":
              ports.put(ServiceType.ANALYTICS, port);
              break;
            case "cbasSSL":
              sslPorts.put(ServiceType.ANALYTICS, port);
              break;
          }
        }
    }

    public Map<ServiceType, Integer> ports() {
        return ports;
    }

    public Map<ServiceType, Integer> sslPorts() {
        return sslPorts;
    }

    public String hostname() {
        return hostname;
    }

    public Map<String, AlternateAddress> alternateAddresses() {
        return alternateAddresses;
    }

    @Override
    public String toString() {
        return "PortInfo{"
            + "ports=" + ports
            + ", sslPorts=" + sslPorts
            + ", hostname='" + hostname
            + ", alternateAddresses=" + alternateAddresses
            + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PortInfo portInfo = (PortInfo) o;

        if (ports != null ? !ports.equals(portInfo.ports) : portInfo.ports != null) return false;
        if (sslPorts != null ? !sslPorts.equals(portInfo.sslPorts) : portInfo.sslPorts != null)
            return false;
        if (alternateAddresses != null ? !alternateAddresses.equals(portInfo.alternateAddresses) : portInfo.alternateAddresses != null)
            return false;
        return hostname != null ? hostname.equals(portInfo.hostname) : portInfo.hostname == null;
    }

    @Override
    public int hashCode() {
        int result = ports != null ? ports.hashCode() : 0;
        result = 31 * result + (sslPorts != null ? sslPorts.hashCode() : 0);
        result = 31 * result + (alternateAddresses != null ? alternateAddresses.hashCode() : 0);
        result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
        return result;
    }
}
