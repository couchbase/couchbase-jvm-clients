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

package com.couchbase.client.core.node;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.topology.KetamaRingNode;

/**
 * This interface defines different hashing strategies used for ketama hashing in memcached buckets.
 *
 * @see StandardMemcachedHashingStrategy#INSTANCE
 * @see Sdk2CompatibleMemcachedHashingStrategy#INSTANCE
 */
@Stability.Internal
public interface MemcachedHashingStrategy {

    /**
     * The hash for each node based on the node information and repetition.
     *
     * @param info the node info
     * @param repetition the repetition
     * @return the hashed node
     */
    String hash(KetamaRingNode info, int repetition);

}
