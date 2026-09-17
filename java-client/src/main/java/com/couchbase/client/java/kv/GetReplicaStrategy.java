/*
 * Copyright 2026 Couchbase, Inc.
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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreGetReplicaStrategy;
import com.couchbase.client.core.api.kv.CoreReplicaIndex;
import com.couchbase.client.core.error.ReplicaIndexCurrentlyUnavailableException;
import com.couchbase.client.core.error.ReplicaIndexOutOfBoundsException;
import com.couchbase.client.core.service.kv.CoreGetReplicaStrategyFromIndex;

import java.util.List;

import static com.couchbase.client.core.util.CbCollections.listOf;

/**
 * Strategy for selecting a replica to read from.
 * <p>
 * Create a new instance by calling one of the static factory methods.
 *
 * @see #any()
 * @see #fromIndex(ReplicaIndex)
 */
public abstract class GetReplicaStrategy {

  /**
   * Returns a strategy that reads from the specified replica.
   *
   * @see FromIndex#withWrap(boolean)
   */
  public static FromIndex fromIndex(ReplicaIndex index) {
    return new FromIndex(index, false);
  }

  /**
   * Returns a strategy that reads from a single available replica.
   * <p>
   * The selection algorithm is unspecified.
   */
  @Stability.Volatile
  public static GetReplicaStrategy any() {
    return fromIndex(ReplicaIndex.FIRST).withWrap(true);
  }

  @Stability.Internal
  public abstract CoreGetReplicaStrategy toCore();

  public static class FromIndex extends GetReplicaStrategy {
    private static final List<CoreReplicaIndex> coreReplicaIndexes = listOf(CoreReplicaIndex.values());

    private final ReplicaIndex index;
    private final boolean wrap;

    private FromIndex(ReplicaIndex index, boolean wrap) {
      this.index = index;
      this.wrap = wrap;
    }

    /**
     * Returns a new instance with the specified wrap setting.
     * <p>
     * Set wrap to true if you want to efficiently read from a single replica, but don't necessarily care which one.
     * <p>
     * When wrap is true:
     * <ul>
     * <li>The specified replica index, modulo the actual number of replicas configured on the bucket,
     * is the starting point for a scan of all available replicas; the result may come from a different replica index than the one specified.
     * <li>{@link ReplicaIndexOutOfBoundsException} is not thrown unless there are no replicas.
     * <li>{@link ReplicaIndexCurrentlyUnavailableException} is not thrown unless all replicas are currently unavailable.
     * </ul>
     * <p>
     * Default value: false
     *
     * @return a new instance with the given wrap value.
     */
    public FromIndex withWrap(boolean wrap) {
      return new FromIndex(index, wrap);
    }

    @Override
    public CoreGetReplicaStrategy toCore() {
      CoreReplicaIndex coreIndex = coreReplicaIndexes.get(index.ordinal());
      return new CoreGetReplicaStrategyFromIndex(coreIndex, wrap);
    }
  }
}
