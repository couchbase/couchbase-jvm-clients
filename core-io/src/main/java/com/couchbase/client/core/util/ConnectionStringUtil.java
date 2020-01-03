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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.core.DnsSrvLookupDisabledEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvLookupFailedEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvRecordsLoadedEvent;
import com.couchbase.client.core.env.SeedNode;

import javax.naming.NameNotFoundException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains various helper methods when dealing with the connection string.
 */
@Stability.Internal
public class ConnectionStringUtil {

    private ConnectionStringUtil() {}

    /**
     * Populates a list of seed nodes from the connection string.
     * <p>
     * Note that this method also performs DNS SRV lookups if the connection string qualifies!
     *
     * @param cs the connection string in its encoded form.
     * @param dnsSrvEnabled true if dns srv is enabled.
     * @param tlsEnabled true if tls is enabled.
     * @return a set of seed nodes populated.
     */
    public static Set<SeedNode> seedNodesFromConnectionString(final String cs, final boolean dnsSrvEnabled,
                                                              final boolean tlsEnabled, final EventBus eventBus) {
        final ConnectionString connectionString = ConnectionString.create(cs);

        if (dnsSrvEnabled && connectionString.isValidDnsSrv()) {
            String srvHostname = connectionString.hosts().get(0).hostname();
            long start = System.nanoTime();
            try {
                // Technically a hostname with the _couchbase._tcp. (and the tls equivalent) does not qualify for
                // a connection string, but we can be good citizens and remove it so the user can still bootstrap.
                if (srvHostname.startsWith(DnsSrv.DEFAULT_DNS_SERVICE)) {
                    srvHostname = srvHostname.replace(DnsSrv.DEFAULT_DNS_SERVICE, "");
                } else if (srvHostname.startsWith(DnsSrv.DEFAULT_DNS_SECURE_SERVICE)) {
                    srvHostname = srvHostname.replace(DnsSrv.DEFAULT_DNS_SECURE_SERVICE, "");
                }

                final List<String> foundNodes = DnsSrv.fromDnsSrv(srvHostname, false, tlsEnabled);
                if (foundNodes.isEmpty()) {
                    throw new IllegalStateException("The loaded DNS SRV list from " + srvHostname + " is empty!");
                }
                Duration took = Duration.ofNanos(System.nanoTime() - start);
                eventBus.publish(new DnsSrvRecordsLoadedEvent(took, foundNodes));
                return foundNodes.stream().map(SeedNode::create).collect(Collectors.toSet());
            } catch (Throwable t) {
                Duration took = Duration.ofNanos(System.nanoTime() - start);
                if (t instanceof NameNotFoundException) {
                    eventBus.publish(new DnsSrvLookupFailedEvent(
                      Event.Severity.INFO,
                      took,
                      null,
                      DnsSrvLookupFailedEvent.Reason.NAME_NOT_FOUND)
                    );
                } else {
                    eventBus.publish(new DnsSrvLookupFailedEvent(
                      Event.Severity.WARN,
                      took,
                      t,
                      DnsSrvLookupFailedEvent.Reason.OTHER
                    ));
                }
                return populateSeedsFromConnectionString(connectionString);
            }
        } else {
            eventBus.publish(new DnsSrvLookupDisabledEvent(dnsSrvEnabled, connectionString.isValidDnsSrv()));
            return populateSeedsFromConnectionString(connectionString);
        }
    }

    /**
     * Extracts the seed nodes from the instantiated connection string.
     *
     * @param connectionString the instantiated connection string.
     * @return the set of seed nodes extracted.
     */
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
