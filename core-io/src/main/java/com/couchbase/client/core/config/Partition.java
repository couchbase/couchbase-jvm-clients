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

/**
 * @deprecated In favor of {@link com.couchbase.client.core.topology.PartitionTopology}
 */
@Deprecated
public class Partition {

    private final short active;
    private final short[] replicas;

    /**
     * Creates a new {@link Partition}.
     *
     * @param active the array index of the active
     * @param replicas the array indexes of the replicas.
     */
    public Partition(short active, short[] replicas) {
        this.active = active;
        this.replicas = replicas;
    }

    public short active() {
        return active;
    }

    public short replica(int num) {
        if (num >= replicas.length) {
            return -2;
        }
        return replicas[num];
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[" + active);
        for (short replica : replicas) {
            result.append(",").append(replica);
        }
        return result + "]";
    }
}
