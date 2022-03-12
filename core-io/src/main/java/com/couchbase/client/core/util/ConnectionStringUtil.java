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
import com.couchbase.client.core.cnc.events.config.ConnectionStringIgnoredEvent;
import com.couchbase.client.core.cnc.events.config.TlsRequiredButNotEnabledEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvLookupDisabledEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvLookupFailedEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvRecordsLoadedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.InvalidArgumentException;

import javax.naming.NameNotFoundException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.env.SecurityConfig.InternalMethods.userSpecifiedTrustSource;
import static com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASES;

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
                } else if (t.getCause() instanceof SocketTimeoutException) {
                    eventBus.publish(new DnsSrvLookupFailedEvent(
                      Event.Severity.INFO,
                      took,
                      null,
                      DnsSrvLookupFailedEvent.Reason.TIMED_OUT)
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
        final Map<String, List<ConnectionString.UnresolvedSocket>> aggregated = new LinkedHashMap<>();
        for (ConnectionString.UnresolvedSocket socket : connectionString.hosts()) {
            if (!aggregated.containsKey(socket.hostname())) {
                aggregated.put(socket.hostname(), new ArrayList<>());
            }
            aggregated.get(socket.hostname()).add(socket);
        }

        Set<SeedNode> seedNodes = aggregated.entrySet().stream().map(entry -> {
            String hostname = entry.getKey();
            Optional<Integer> kvPort = Optional.empty();
            Optional<Integer> managerPort = Optional.empty();

            for (ConnectionString.UnresolvedSocket socket : entry.getValue()) {
                if (socket.portType().isPresent()) {
                    if (socket.portType().get() == ConnectionString.PortType.KV) {
                        kvPort = Optional.of(socket.port());
                    } else if (socket.portType().get() == ConnectionString.PortType.MANAGER) {
                        managerPort = Optional.of(socket.port());
                    }
                } else if (socket.port() != 0) {
                    kvPort = Optional.of(socket.port());
                }
            }

            return SeedNode.create(hostname, kvPort, managerPort);
        }).collect(Collectors.toSet());

        sanityCheckSeedNodes(connectionString.original(), seedNodes);
        return seedNodes;
    }

    /**
     * Sanity check the seed node list for common errors that can be caught early on.
     *
     * @param seedNodes the seed nodes to verify.
     */
    private static void sanityCheckSeedNodes(final String connectionString, final Set<SeedNode> seedNodes) {
        for (SeedNode seedNode : seedNodes) {
            if (seedNode.kvPort().isPresent()) {
                if (seedNode.kvPort().get() == 8091 || seedNode.kvPort().get() == 18091) {
                    String recommended = connectionString
                      .replace(":8091", "")
                      .replace(":18091", "");
                    throw new InvalidArgumentException("Specifying 8091 or 18091 in the connection string \"" + connectionString + "\" is " +
                      "likely not what you want (it would connect to key/value via the management port which does not work). Please omit " +
                      "the port and use \"" + recommended + "\" instead.", null, null);
                }
            }
        }
    }

  /**
   * Returns true if the addresses indicate this is a Couchbase Capella cluster.
   */
  public static boolean isCapella(ConnectionString connectionString) {
    return connectionString.hosts().stream()
        .allMatch(it -> it.hostname().endsWith(".cloud.couchbase.com"));
  }

  /**
   * Returns a synthetic connection string corresponding to the seed nodes.
   */
  public static String asConnectionString(Collection<SeedNode> nodes) {
    return nodes.stream()
        .map(ConnectionStringUtil::asConnectionStringAddress)
        .collect(Collectors.joining(","));
  }

  private static String asConnectionStringAddress(SeedNode node) {
    StringBuilder sb = new StringBuilder(node.address());
    // Connection string can have only one port per address.
    // If both KV and manager ports are present, prefer the KV port.
    if (node.kvPort().isPresent()) {
      // "=kv" is the default, but it doesn't hurt to be explicit
      sb.append(":").append(node.kvPort().get()).append("=kv");
    } else {
      node.clusterManagerPort().ifPresent(it ->
          sb.append(":").append(it).append("=manager")
      );
    }
    return sb.toString();
  }

  public static void checkConnectionString(CoreEnvironment env, boolean ownsEnvironment, ConnectionString connStr) {
    boolean tls = env.securityConfig().tlsEnabled();

    // Let the user know if we're ignoring bits of the connection string
    // because they're incompatible with a shared cluster environment.
    if (!ownsEnvironment) {
      if (!tls && connStr.scheme() == COUCHBASES) {
        env.eventBus().publish(ConnectionStringIgnoredEvent.ignoringScheme(connStr));
      }
      if (!connStr.params().isEmpty()) {
        env.eventBus().publish(ConnectionStringIgnoredEvent.ignoringParameters(connStr));
      }
    }

    boolean capella = isCapella(connStr);
    if (tls && !userSpecifiedTrustSource(env.securityConfig()) && !capella) {
      // Default trust source only works with Capella.
      throw InvalidArgumentException.fromMessage(
          "When TLS is enabled, the cluster environment's security config must specify" +
              " either the Certificate Authority certificate(s) to trust," +
              " or the trust manager factory to use." +
              " (Unless connecting to cloud.couchbase.com.)"
      );
    }

    if (capella && !tls) {
      // Can't connect to Capella without TLS. Until we determine
      // a better way of detecting and handling this, log a warning
      // and continue marching straight off the cliff.
      env.eventBus().publish(ownsEnvironment
          ? TlsRequiredButNotEnabledEvent.forOwnedEnvironment()
          : TlsRequiredButNotEnabledEvent.forSharedEnvironment()
      );
    }
  }
}
