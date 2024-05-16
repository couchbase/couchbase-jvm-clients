/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.core.topology.KetamaRingNode;
import com.couchbase.client.core.util.HostAndPort;
import com.couchbase.client.core.util.NetworkAddress;

import static java.util.Objects.requireNonNull;

/**
 * This Memcached Hashing Strategy is compatible with the SDK 2 "DefaultMemcachedHashingStrategy".
 * <p>
 * It should only be used if code is migrated from Java SDK 2 to SDK 3 and access to cached documents need to be
 * preserved. For everything else (especially for interop with libcouchbase-based SDKs) we strongly recommend the
 * default {@link StandardMemcachedHashingStrategy}.
 */
public class Sdk2CompatibleMemcachedHashingStrategy implements MemcachedHashingStrategy {

  public static Sdk2CompatibleMemcachedHashingStrategy INSTANCE = new Sdk2CompatibleMemcachedHashingStrategy();

  private Sdk2CompatibleMemcachedHashingStrategy() {}

  @Override
  public String hash(final KetamaRingNode info, final int repetition) {
    HostAndPort authority = requireNonNull(
      info.ketamaAuthority(),
      "Oops, didn't filter out nodes with missing ketama authority"
    );

    // Note: in SDK 3 we NEVER resolve an address or perform a reverse DNS lookup. But because for
    // ketama hashing we need to do it in order to return the same hostnames that SDK 2 does in
    // the case of this legacy sdk 2 compatible strategy.
    return NetworkAddress.create(authority.host()).hostname() + "-" + repetition;
  }

}
