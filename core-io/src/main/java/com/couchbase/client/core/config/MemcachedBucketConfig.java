/*
 * Copyright (c) 2016-2017 Couchbase, Inc.
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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JacksonInject;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.node.NodeIdentifier;
import com.couchbase.client.core.topology.KetamaRing;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MemcachedBucketConfig extends AbstractBucketConfig {

    private final KetamaRing<NodeInfo> ketamaRing;

    /**
     * Creates a new {@link MemcachedBucketConfig}.
     *
     * @param env the environment to use.
     * @param rev the revision of the config.
     * @param name the name of the bucket.
     * @param uri the URI for this bucket.
     * @param streamingUri the streaming URI for this bucket.
     * @param nodeInfos related node information.
     * @param portInfos port info for the nodes, including services.
     */
    @JsonCreator
    public MemcachedBucketConfig(
            @JacksonInject("env") CoreEnvironment env,
            @JsonProperty("rev") long rev,
            @JsonProperty("revEpoch") long revEpoch,
            @JsonProperty("uuid") String uuid,
            @JsonProperty("name") String name,
            @JsonProperty("uri") String uri,
            @JsonProperty("streamingUri") String streamingUri,
            @JsonProperty("nodes") List<NodeInfo> nodeInfos,
            @JsonProperty("nodesExt") List<PortInfo> portInfos,
            @JsonProperty("bucketCapabilities") List<BucketCapabilities> bucketCapabilities,
            @JsonProperty("clusterCapabilities") Map<String, Set<ClusterCapabilities>> clusterCapabilities,
            @JacksonInject("origin") String origin) {
        super(uuid, name, BucketNodeLocator.KETAMA, uri, streamingUri, nodeInfos, portInfos, bucketCapabilities,
          origin, clusterCapabilities, rev, revEpoch);

        this.ketamaRing = KetamaRing.create(
            nodes(),
            env.ioConfig().memcachedHashingStrategy()
        );
    }

    @Override
    public boolean tainted() {
        return false;
    }

    @Override
    public BucketType type() {
        return BucketType.MEMCACHED;
    }

    /**
     * @deprecated Please use {@link #nodeForKey(byte[])} for Ketama lookups instead.
     */
    @Deprecated
    public SortedMap<Long, NodeInfo> ketamaNodes() {
        return ketamaRing.toMap();
    }

    // Visible for testing
    KetamaRing<NodeInfo> ketamaRing() {
        return ketamaRing;
    }

    /**
     * @deprecated Please use {@link #nodeForKey(byte[])}.identifier() instead.
     */
    @Deprecated
    public NodeIdentifier nodeForId(final byte[] id) {
        return nodeForKey(id).identifier();
    }

    public NodeInfo nodeForKey(final byte[] id) {
        return ketamaRing.get(id);
    }

    @Override
    public boolean hasFastForwardMap() {
        return false;
    }

    /**
     * Note that dumping the whole Ketama ring is pretty much useless, so here we focus on just dumping all the nodes
     * that participate in the cluster instead.
     */
    @Override
    public String toString() {
        return "MemcachedBucketConfig{" +
          "name='" + redactMeta(name()) + '\'' +
          ", version=" + version() +
          ", nodes=" + redactSystem(nodes()) +
          '}';
    }

}
