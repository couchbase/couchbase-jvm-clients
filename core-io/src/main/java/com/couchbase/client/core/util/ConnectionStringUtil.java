/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.util;

import com.couchbase.client.core.env.SeedNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ConnectionStringUtil {
    private ConnectionStringUtil() {}

    public static Set<SeedNode> seedNodesFromConnectionString(final String cs, final boolean dnsSrvEnabled) {
        final ConnectionString connectionString = ConnectionString.create(cs);

        if (dnsSrvEnabled && connectionString.isValidDnsSrv()) {
            boolean isEncrypted = connectionString.scheme() == ConnectionString.Scheme.COUCHBASES;
            String dnsHostname = connectionString.hosts().get(0).hostname();
            try {
                List<String> foundNodes = DnsSrv.fromDnsSrv("", false, isEncrypted, dnsHostname);
                if (foundNodes.isEmpty()) {
                    throw new IllegalStateException("The loaded DNS SRV list from " + dnsHostname + " is empty!");
                }
                return foundNodes.stream().map(SeedNode::create).collect(Collectors.toSet());
            } catch (Exception ex) {
                return populateSeedsFromConnectionString(connectionString);
            }
        } else {
            return populateSeedsFromConnectionString(connectionString);
        }
    }

    private static Set<SeedNode> populateSeedsFromConnectionString(final ConnectionString connectionString) {
        return connectionString
                .hosts()
                .stream()
                .map(a -> SeedNode.create(
                        a.hostname(),
                        a.port() > 0 ? Optional.of(a.port()) : Optional.empty(),
                        Optional.empty()
                ))
                .collect(Collectors.toSet());
    }
}
