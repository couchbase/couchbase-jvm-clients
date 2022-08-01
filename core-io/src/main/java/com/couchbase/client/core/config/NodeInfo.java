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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import reactor.util.annotation.Nullable;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

/**
 * Default implementation of {@link NodeInfo}.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeInfo {

    private final String hostname;
    private final Map<ServiceType, Integer> directServices;
    private final Map<ServiceType, Integer> sslServices;
    private final Map<String, AlternateAddress> alternateAddresses;
    private final NodeIdentifier nodeIdentifier;
    private int configPort;


    /**
     * Creates a new {@link NodeInfo} with no SSL services.
     *
     * @param viewUri  the URI of the view service.
     * @param hostname the hostname of the node.
     * @param ports    the port list of the node services.
     */
    @JsonCreator
    public NodeInfo(
        @JsonProperty("couchApiBase") String viewUri,
        @JsonProperty("hostname") String hostname,
        @JsonProperty("ports") Map<String, Integer> ports,
        @JsonProperty("alternateAddresses") Map<String, AlternateAddress> alternateAddresses) {
        if (hostname == null) {
            throw InvalidArgumentException.fromMessage("NodeInfo hostname cannot be null");
        }
        this.alternateAddresses = alternateAddresses == null
            ? Collections.emptyMap()
            : alternateAddresses;

        try {
            this.hostname = trimPort(hostname);
        } catch (Exception e) {
            throw new CouchbaseException("Could not analyze hostname from config.", e);
        }
        this.directServices = parseDirectServices(viewUri, ports);
        this.sslServices = new HashMap<>();
        this.nodeIdentifier = new NodeIdentifier(this.hostname, directServices.get(ServiceType.MANAGER));
    }

    /**
     * Creates a new {@link NodeInfo} with SSL services.
     *
     * @param hostname the hostname of the node.
     * @param direct   the port list of the direct node services.
     * @param ssl      the port list of the ssl node services.
     */
    public NodeInfo(String hostname, Map<ServiceType, Integer> direct,
                    Map<ServiceType, Integer> ssl, Map<String, AlternateAddress> alternateAddresses) {
        if (hostname == null) {
            throw InvalidArgumentException.fromMessage("NodeInfo hostname cannot be null");
        }

        this.hostname = hostname;
        this.directServices = direct;
        this.sslServices = ssl;
        this.alternateAddresses = alternateAddresses == null
            ? Collections.emptyMap()
            : alternateAddresses;

        Integer directManagerPort = directServices.get(ServiceType.MANAGER);
        Integer sslManagerPort = sslServices.get(ServiceType.MANAGER);
        if (directManagerPort != null) {
            this.nodeIdentifier = new NodeIdentifier(this.hostname, directManagerPort);
        } else if (sslManagerPort != null) {
            this.nodeIdentifier = new NodeIdentifier(this.hostname, sslManagerPort);
        } else {
            throw new IllegalStateException("A config must have at least a non-ssl or a ssl manager port defined!");
        }

    }

    public String hostname() {
        return hostname;
    }

    public NodeIdentifier identifier() {
        return nodeIdentifier;
    }

    public Map<ServiceType, Integer> services() {
        return directServices;
    }

    public Map<ServiceType, Integer> sslServices() {
        return sslServices;
    }

    public Map<String, AlternateAddress> alternateAddresses() {
        return alternateAddresses;
    }

    private Map<ServiceType, Integer> parseDirectServices(@Nullable final String viewUri,
                                                          @Nullable final Map<String, Integer> input) {
        Map<ServiceType, Integer> services = new HashMap<>();
        services.put(ServiceType.MANAGER, configPort);

        if (input != null) {
            for (Map.Entry<String, Integer> entry : input.entrySet()) {
                String type = entry.getKey();
                Integer port = entry.getValue();
                if (type.equals("direct")) {
                    services.put(ServiceType.KV, port);
                }
            }
        }

        if (viewUri != null) {
            services.put(ServiceType.VIEWS, URI.create(viewUri).getPort());
        }

        return services;
    }

    private String trimPort(final String hostname) {
        String[] parts = hostname.split(":");
        configPort = Integer.parseInt(parts[parts.length - 1]);

        if (parts.length > 2) {
            // Handle IPv6 syntax
            String assembledHost = "";
            for (int i = 0; i < parts.length - 1; i++) {
                assembledHost += parts[i];
                if (parts[i].endsWith("]")) {
                    break;
                } else {
                    assembledHost += ":";
                }
            }
            if (assembledHost.startsWith("[") && assembledHost.endsWith("]")) {
                return assembledHost.substring(1, assembledHost.length() - 1);
            }
            if (assembledHost.endsWith(":")) {
                assembledHost = assembledHost.substring(0, assembledHost.length() - 1);
            }
            return assembledHost;
        } else {
            // Simple IPv4 Handling
            return parts[0];
        }
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
            "host=" + redactSystem(hostname) +
            ", ports=" + redactSystem(directServices) +
            ", securePorts=" + redactSystem(sslServices) +
            ", aa=" + redactSystem(alternateAddresses) +
            ", configPort=" + redactSystem(configPort) +
            ", nodeIdentifier=" + redactSystem(nodeIdentifier) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return configPort == nodeInfo.configPort && Objects.equals(hostname, nodeInfo.hostname)
          && Objects.equals(directServices, nodeInfo.directServices)
          && Objects.equals(sslServices, nodeInfo.sslServices)
          && Objects.equals(alternateAddresses, nodeInfo.alternateAddresses)
          && Objects.equals(nodeIdentifier, nodeInfo.nodeIdentifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, directServices, sslServices, alternateAddresses, nodeIdentifier, configPort);
    }

}
