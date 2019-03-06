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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

/**
 * Represents the partition information for a bucket.
 *
 * @since 1.1.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PartitionInfo {

    private final int numberOfReplicas;
    private final String[] partitionHosts;
    private final List<Partition> partitions;
    private final List<Partition> forwardPartitions;
    private final boolean tainted;

    PartitionInfo(
        @JsonProperty("numReplicas") int numberOfReplicas,
        @JsonProperty("serverList") List<String> partitionHosts,
        @JsonProperty("vBucketMap") List<List<Short>> partitions,
        @JsonProperty("vBucketMapForward") List<List<Short>> forwardPartitions) {
        this.numberOfReplicas = numberOfReplicas;
        this.partitionHosts = partitionHosts.toArray(new String[partitionHosts.size()]);
        this.partitions = fromPartitionList(partitions);
        if (forwardPartitions != null && !forwardPartitions.isEmpty()) {
            this.forwardPartitions = fromPartitionList(forwardPartitions);
            this.tainted = true;
        } else {
            this.forwardPartitions = null;
            this.tainted = false;
        }
    }

    public boolean hasFastForwardMap() {
        return forwardPartitions != null;
    }

    public int numberOfReplicas() {
        return numberOfReplicas;
    }

    public String[] partitionHosts() {
        return partitionHosts;
    }

    public List<Partition> partitions() {
        return partitions;
    }

    public List<Partition> forwardPartitions() {
        return forwardPartitions;
    }

    public boolean tainted() {
        return tainted;
    }

    private static List<Partition> fromPartitionList(List<List<Short>> input) {
        List<Partition> partitions = new ArrayList<Partition>();
        if (input == null) {
            return partitions;
        }

        for (List<Short> partition : input) {
            short master = partition.remove(0);
            short[] replicas = new short[partition.size()];
            int i = 0;
            for (short replica : partition) {
                replicas[i++] = replica;
            }
            partitions.add(new Partition(master, replicas));
        }
        return partitions;
    }

    @Override
    public String toString() {
        return "PartitionInfo{"
            + "numberOfReplicas=" + numberOfReplicas
            + ", partitionHosts=" + Arrays.toString(partitionHosts)
            + ", partitions=" + partitions
            + ", tainted=" + tainted
            + '}';
    }
}
