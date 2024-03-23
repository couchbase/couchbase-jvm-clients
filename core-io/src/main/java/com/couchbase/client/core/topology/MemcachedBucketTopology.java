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
import com.couchbase.client.core.node.MemcachedHashingStrategy;

import java.util.List;
import java.util.Set;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class MemcachedBucketTopology extends AbstractBucketTopology {
  private final KetamaRing<HostAndServicePorts> ketamaRing;

  private final MemcachedHashingStrategy hashingStrategy;

  public MemcachedBucketTopology(
    String uuid,
    String name,
    Set<BucketCapability> capabilities,
    List<HostAndServicePorts> nodes,
    KetamaRing<HostAndServicePorts> ketamaRing,
    MemcachedHashingStrategy hashingStrategy
  ) {
    super(uuid, name, capabilities, nodes);
    this.ketamaRing = requireNonNull(ketamaRing);
    this.hashingStrategy = requireNonNull(hashingStrategy);
  }

  public HostAndServicePorts nodeForKey(final byte[] id) {
    return ketamaRing.get(id);
  }

  // Visible for testing
  KetamaRing<HostAndServicePorts> ketamaRing() {
    return ketamaRing;
  }

  // just so we can easily convert this into a legacy MemcachedBucketConfig
  @Stability.Internal
  public MemcachedHashingStrategy hashingStrategy() {
    return hashingStrategy;
  }

  @Override
  public String toString() {
    return "MemcachedBucketTopology{" +
      "name='" + redactMeta(name()) + '\'' +
      ", uuid='" + uuid() + '\'' +
      ", capabilities=" + capabilities() +
      ", hashingStrategy=" + hashingStrategy.getClass().getSimpleName() +
      '}';
  }
}
