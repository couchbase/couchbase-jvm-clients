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

import com.couchbase.client.core.service.ServiceType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;

public abstract class AbstractBucketConfig implements BucketConfig {

    private final String uuid;
    private final String name;
    private final BucketNodeLocator locator;
    private final String uri;
    private final String streamingUri;
    private final List<NodeInfo> nodeInfo;
    private final int enabledServices;
    private final Set<BucketCapabilities> bucketCapabilities;
    private final Map<ServiceType, Set<ClusterCapabilities>> clusterCapabilities;
    private final String origin;
    private final List<PortInfo> portInfos;
    private final long rev;

    protected AbstractBucketConfig(String uuid, String name, BucketNodeLocator locator, String uri, String streamingUri,
                                   List<NodeInfo> nodeInfos, List<PortInfo> portInfos,
                                   List<BucketCapabilities> bucketCapabilities, String origin,
                                   Map<String, Set<ClusterCapabilities>> clusterCapabilities,
                                   long rev
    ) {
        this.uuid = uuid;
        this.name = name;
        this.locator = locator;
        this.uri = uri;
        this.streamingUri = streamingUri;
        this.bucketCapabilities = convertBucketCapabilities(bucketCapabilities);
        this.clusterCapabilities = convertClusterCapabilities(clusterCapabilities);
        this.origin = origin;
        this.rev = rev;
        this.portInfos = portInfos == null ? Collections.emptyList() : portInfos;
        this.nodeInfo = portInfos == null ? nodeInfos : nodeInfoFromExtended(portInfos, nodeInfos);
        int es = 0;
        for (NodeInfo info : nodeInfo) {
            for (ServiceType type : info.services().keySet()) {
                es |= 1 << type.ordinal();
            }
            for (ServiceType type : info.sslServices().keySet()) {
                es |= 1 << type.ordinal();
            }
        }
        this.enabledServices = es;
    }

    static Set<BucketCapabilities> convertBucketCapabilities(final List<BucketCapabilities> input) {
        if (isNullOrEmpty(input)) {
            return Collections.emptySet();
        }
        return EnumSet.copyOf(input.stream().filter(Objects::nonNull).collect(Collectors.toSet()));
    }

    static Map<ServiceType, Set<ClusterCapabilities>> convertClusterCapabilities(
      final Map<String, Set<ClusterCapabilities>> input) {

        Map<ServiceType, Set<ClusterCapabilities>> result = new HashMap<>();
        result.put(ServiceType.MANAGER, Collections.emptySet());
        result.put(ServiceType.QUERY, Collections.emptySet());
        result.put(ServiceType.VIEWS, Collections.emptySet());
        result.put(ServiceType.KV, Collections.emptySet());
        result.put(ServiceType.SEARCH, Collections.emptySet());
        result.put(ServiceType.ANALYTICS, Collections.emptySet());
        result.put(ServiceType.EVENTING, Collections.emptySet());

        if (input == null) {
            return result;
        }

        for (Map.Entry<String, Set<ClusterCapabilities>> entry : input.entrySet()) {
            Set<ClusterCapabilities> capabilities = entry
              .getValue()
              .stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toSet());

            if (capabilities.isEmpty()) {
                continue;
            }

            EnumSet<ClusterCapabilities> filtered = EnumSet.copyOf(entry
              .getValue()
              .stream()
              .filter(Objects::nonNull)
              .collect(Collectors.toSet()));

            switch (entry.getKey()) {
                case "mgmt":
                    result.put(ServiceType.MANAGER, filtered);
                    break;
                case "n1ql":
                    result.put(ServiceType.QUERY, filtered);
                    break;
                case "capi":
                    result.put(ServiceType.VIEWS, filtered);
                    break;
                case "kv":
                    result.put(ServiceType.KV, filtered);
                    break;
                case "fts":
                    result.put(ServiceType.SEARCH, filtered);
                    break;
                case "cbas":
                    result.put(ServiceType.ANALYTICS, filtered);
                    break;
                case "eventing":
                    result.put(ServiceType.EVENTING, filtered);
                    break;
            }
        }
        return result;
    }

    /**
     * Helper method to create the {@link NodeInfo}s from from the extended node information.
     *
     * <p>In older server versions (&lt; 3.0.2) the nodesExt part does not carry a hostname, so as
     * a fallback the hostname is loaded from the node info if needed.</p>
     *
     * @param nodesExt the extended information.
     * @return the generated node infos.
     */
    private List<NodeInfo> nodeInfoFromExtended(final List<PortInfo> nodesExt, final List<NodeInfo> nodeInfos) {
        List<NodeInfo> converted = new ArrayList<>(nodesExt.size());
        for (int i = 0; i < nodesExt.size(); i++) {
            String hostname = nodesExt.get(i).hostname();

            // Since nodeInfo and nodesExt might not be the same size, this can be null!
            NodeInfo nodeInfo = i >= nodeInfos.size() ? null : nodeInfos.get(i);

            if (hostname == null) {
                if (nodeInfo != null) {
                    hostname = nodeInfo.hostname();
                } else {
                    // If hostname missing, then node configured using localhost
                    // TODO: LOGGER.debug("Hostname is for nodesExt[{}] is not available, falling back to origin.", i);
                    hostname = origin;
                }
            }
            Map<ServiceType, Integer> ports = new HashMap<>(nodesExt.get(i).ports());
            Map<ServiceType, Integer> sslPorts = new HashMap<>(nodesExt.get(i).sslPorts());
            Map<String, AlternateAddress> aa = new HashMap<>(nodesExt.get(i).alternateAddresses());

            // this is an ephemeral bucket (not supporting views), don't enable views!
            if (!bucketCapabilities.contains(BucketCapabilities.COUCHAPI)) {
                ports.remove(ServiceType.VIEWS);
                sslPorts.remove(ServiceType.VIEWS);
            }

            // make sure only kv nodes are added if they are actually also in the nodes
            // list and not just in nodesExt, since the kv service might be available
            // on the cluster but not yet enabled for this specific bucket.
            //
            // Since in the past we have seen that views only work properly on a node if
            // kv is available, don't route view ops here as well.
            if (nodeInfo == null) {
                ports.remove(ServiceType.KV);
                sslPorts.remove(ServiceType.KV);

                ports.remove(ServiceType.VIEWS);
                sslPorts.remove(ServiceType.VIEWS);
            }

            converted.add(new NodeInfo(hostname, ports, sslPorts, aa));
        }
        return converted;
    }

    @Override
    public String uuid() {
        return uuid;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public BucketNodeLocator locator() {
        return locator;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public String streamingUri() {
        return streamingUri;
    }

    @Override
    public List<NodeInfo> nodes() {
        return nodeInfo;
    }

    @Override
    public long rev() {
        return rev;
    }

    @Override
    public boolean serviceEnabled(ServiceType type) {
        return (enabledServices & (1 << type.ordinal())) != 0;
    }

    @Override
    public Map<ServiceType, Set<ClusterCapabilities>> clusterCapabilities() {
        return clusterCapabilities;
    }

    @Override
    public Set<BucketCapabilities> bucketCapabilities() {
        return bucketCapabilities;
    }

    @Override
    public List<PortInfo> portInfos() {
        return portInfos;
    }
}
