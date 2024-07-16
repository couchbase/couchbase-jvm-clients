/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.topology;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.HostAndPort;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbCollections.transformValues;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

@Stability.Internal
class HostAndServicePortsParser {
  private HostAndServicePortsParser() {
    throw new AssertionError("not instantiable");
  }

  /**
   * Parses one element of the "nodesExt" array. Returns a map where the keys are the
   * external address network names (plus the implicit "default" network),
   * and each value is the candidate NodeInfo object for the associated network.
   *
   * @param portSelector Determines whether the returned node info has TLS ports or non-TLS ports.
   */
  public static Map<NetworkResolution, HostAndServicePorts> parse(
    ObjectNode json,
    PortSelector portSelector
  ) {
    Map<NetworkResolution, HostAndRawServicePorts> raw = parseIntermediate(json);
    HostAndPort ketamaAuthority = getKetamaAuthority(raw);
    NodeIdentifier id = getId(raw);

    return transformValues(raw, value ->
      new HostAndServicePorts(
        value.host,
        portSelector.selectPorts(value.rawServicePorts),
        id,
        ketamaAuthority
      )
    );
  }

  /**
   * Returns the host and non-TLS port for the KV service on the default network,
   * or null if there's no such port.
   */
  @Nullable
  private static HostAndPort getKetamaAuthority(Map<NetworkResolution, HostAndRawServicePorts> networkToNodeInfo) {
    HostAndRawServicePorts defaultNodeMap = networkToNodeInfo.get(NetworkResolution.DEFAULT);
    if (defaultNodeMap == null) {
      return null;
    }

    Integer nonTlsKvPort = getPort(defaultNodeMap, PortSelector.NON_TLS, ServiceType.KV);
    if (nonTlsKvPort == null) {
      return null;
    }

    return new HostAndPort(defaultNodeMap.host, nonTlsKvPort);
  }

  /**
   * Returns an ID consisting of the host and manager port on the default network.
   * <p>
   * Depending on which ports the server advertises, it might be a TLS or non-TLS port.
   * This must not matter though, since this is just for uniquely identifying nodes,
   * and not for making network connections.
   *
   * @throws CouchbaseException If the default network has no manager ports for the node
   */
  private static NodeIdentifier getId(
    Map<NetworkResolution, HostAndRawServicePorts> networkToNodeInfo
  ) {
    HostAndRawServicePorts defaultNodeMap = networkToNodeInfo.get(NetworkResolution.DEFAULT);
    if (defaultNodeMap == null) {
      throw new CouchbaseException("Network map is missing entry for default network.");
    }

    Integer idPort = defaultIfNull(
      getPort(defaultNodeMap, PortSelector.NON_TLS, ServiceType.MANAGER),
      () -> getPort(defaultNodeMap, PortSelector.TLS, ServiceType.MANAGER)
    );

    if (idPort == null) {
      throw new CouchbaseException(
        "Cluster topology has no manager port on the default network for node: " +
          redactSystem(networkToNodeInfo)
      );
    }

    return new NodeIdentifier(defaultNodeMap.host, idPort);
  }

  @Nullable
  private static Integer getPort(HostAndRawServicePorts nodeMap, PortSelector portSelector, ServiceType serviceType) {
    Map<ServiceType, Integer> ports = portSelector.selectPorts(nodeMap.rawServicePorts);
    return ports.get(serviceType);
  }

  private static Map<NetworkResolution, HostAndRawServicePorts> parseIntermediate(ObjectNode json) {
    Map<NetworkResolution, HostAndRawServicePorts> result = new HashMap<>();

    HostAndRawServicePorts defaultInfo = parseOne(json, "services");
    result.put(NetworkResolution.DEFAULT, defaultInfo);

    json.path("alternateAddresses").fields().forEachRemaining(it -> {
      NetworkResolution network = NetworkResolution.valueOf(it.getKey());
      HostAndRawServicePorts alternate = parseOne((ObjectNode) it.getValue(), "ports");

      // If the alternate has at least one port, then no other services
      // are available on that interface, and the SDK MUST NOT
      // use ports from the default config.

      // The server MAY advertise an alternate address with no ports
      // if all ports are the same as on the default network.
      // However, as of March 2024 no server version uses this optimization.
      // Nevertheless:
      if (alternate.rawServicePorts.isEmpty()) {
        alternate = new HostAndRawServicePorts(alternate.host, defaultInfo.rawServicePorts);
      }

      result.put(network, alternate);
    });

    return result;
  }

  /**
   * @param portsFieldName because of course it's different when parsing alternate addresses :-/
   */
  private static HostAndRawServicePorts parseOne(
    ObjectNode json,
    String portsFieldName
  ) {
    // Nodes where "thisNode" is true don't have a "hostname" in the original config,
    // but we patched in a synthetic "hostname" field earlier.
    String host = json.path("hostname").textValue();

    // Apparently, ancient versions of Couchbase (like, 3.x) could omit the hostname field,
    // and the client would have to get it from the `nodes` list.
    // With Couchbase 5.0, that doesn't happen anymore. Sanity check, just in case:
    if (host == null) {
      throw new CouchbaseException("Couchbase server version is too old for this SDK; nodesExt entry is missing 'hostname' field.");
    }

    return new HostAndRawServicePorts(
      host,
      parseServices((ObjectNode) json.get(portsFieldName))
    );
  }

  private static final TypeReference<Map<String, Integer>> MAP_STRING_TO_INT = new TypeReference<Map<String, Integer>>() {};

  private static Map<String, Integer> parseServices(@Nullable ObjectNode servicesNode) {
    return servicesNode == null
      ? emptyMap()
      : Mapper.convertValue(servicesNode, MAP_STRING_TO_INT);
  }

  private static class HostAndRawServicePorts {
    private final String host;
    private final Map<String, Integer> rawServicePorts;

    /**
     * @param rawServicePorts unfiltered, straight from the config json
     */
    public HostAndRawServicePorts(String host, Map<String, Integer> rawServicePorts) {
      this.host = requireNonNull(host);
      this.rawServicePorts = requireNonNull(rawServicePorts);
    }
  }
}
