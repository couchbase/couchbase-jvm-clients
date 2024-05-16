/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import static java.util.Objects.requireNonNull;

/**
 * The default strategy, compatible with libcouchbase and related SDKs.
 */
public class StandardMemcachedHashingStrategy implements MemcachedHashingStrategy {

    public static final StandardMemcachedHashingStrategy INSTANCE = new StandardMemcachedHashingStrategy();

    private StandardMemcachedHashingStrategy() {
    }

    @Override
    public String hash(final KetamaRingNode info, final int repetition) {
        HostAndPort authority = requireNonNull(
            info.ketamaAuthority(),
            "Oops, didn't filter out nodes with missing ketama authority"
        );

        // Compatibility note: libcouchbase encloses the host in square brackets
        // if it is an IPv6 literal (contains colons). The Java SDK doesn't do that.
        //
        //   return authority.format() + "-" + repetition; // <-- Here's what libcouchbase does!

        return authority.host() + ":" + authority.port() + "-" + repetition;
    }

}
