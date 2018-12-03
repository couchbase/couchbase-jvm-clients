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

package com.couchbase.client.core.config;

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AlternateAddress {

    private final NetworkAddress hostname;
    private final String rawHostname;
    private final Map<ServiceType, Integer> directServices;
    private final Map<ServiceType, Integer> sslServices;

    @JsonCreator
    public AlternateAddress(
        @JsonProperty("hostname") String hostname,
        @JsonProperty("ports") Map<String, Integer> ports) {
        try {
            this.rawHostname = hostname;
            this.hostname = NetworkAddress.create(rawHostname);
        } catch (Exception e) {
            throw new CouchbaseException("Could not analyze hostname from config.", e);
        }

        this.directServices = new HashMap<>();
        this.sslServices = new HashMap<>();
        if (ports != null && !ports.isEmpty()) {
            PortInfo.extractPorts(ports, directServices, sslServices);
        }
    }

    public NetworkAddress hostname() {
        return hostname;
    }

    public String rawHostname() {
        return rawHostname;
    }

    public Map<ServiceType, Integer> services() {
        return directServices;
    }

    public Map<ServiceType, Integer> sslServices() {
        return sslServices;
    }

    @Override
    public String toString() {
        return "DefaultAlternateAddress{" +
            "hostname=" + hostname +
            ", rawHostname='" + rawHostname + '\'' +
            ", directServices=" + directServices +
            ", sslServices=" + sslServices +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AlternateAddress that = (AlternateAddress) o;

        if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) {
            return false;
        }
        if (rawHostname != null ? !rawHostname.equals(that.rawHostname) : that.rawHostname != null) {
            return false;
        }
        if (directServices != null ? !directServices.equals(that.directServices) : that.directServices != null) {
            return false;
        }
        return sslServices != null ? sslServices.equals(that.sslServices) : that.sslServices == null;
    }

    @Override
    public int hashCode() {
        int result = hostname != null ? hostname.hashCode() : 0;
        result = 31 * result + (rawHostname != null ? rawHostname.hashCode() : 0);
        result = 31 * result + (directServices != null ? directServices.hashCode() : 0);
        result = 31 * result + (sslServices != null ? sslServices.hashCode() : 0);
        return result;
    }
}
