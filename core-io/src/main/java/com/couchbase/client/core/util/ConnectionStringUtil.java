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
import com.couchbase.client.core.cnc.events.config.TlsRequiredButNotEnabledEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvLookupDisabledEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvLookupFailedEvent;
import com.couchbase.client.core.cnc.events.core.DnsSrvRecordsLoadedEvent;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.util.ConnectionString.PortType;
import com.couchbase.client.core.util.ConnectionString.UnresolvedSocket;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.ConnectionString.PortType.KV;
import static com.couchbase.client.core.util.ConnectionString.PortType.MANAGER;
import static com.couchbase.client.core.util.ConnectionString.PortType.PROTOSTELLAR;
import static com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASE;
import static com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASE2;
import static com.couchbase.client.core.util.ConnectionString.Scheme.COUCHBASES;
import static java.util.stream.Collectors.groupingBy;

/**
 * Contains various helper methods when dealing with the connection string.
 */
@Stability.Internal
public class ConnectionStringUtil {
  private ConnectionStringUtil() {
  }

  /**
   * Populates a list of seed nodes from the connection string.
   * <p>
   * Note that this method also performs DNS SRV lookups if the connection string qualifies!
   *
   * @param connectionString the connection string in its encoded form.
   * @param dnsSrvEnabled true if dns srv is enabled.
   * @param tlsEnabled true if tls is enabled.
   * @return a set of seed nodes populated.
   */
  public static Set<SeedNode> seedNodesFromConnectionString(final ConnectionString connectionString, final boolean dnsSrvEnabled,
                                                            final boolean tlsEnabled, final EventBus eventBus) {
    if (dnsSrvEnabled && connectionString.isValidDnsSrv()) {
      String srvHostname = connectionString.hosts().get(0).host();
      NanoTimestamp start = NanoTimestamp.now();
      try {
        // Technically a hostname with the _couchbase._tcp. (and the tls equivalent) does not qualify for
        // a connection string, but we can be good citizens and remove it so the user can still bootstrap.
        if (srvHostname.startsWith(DnsSrv.DEFAULT_DNS_SERVICE)) {
          srvHostname = srvHostname.replace(DnsSrv.DEFAULT_DNS_SERVICE, "");
        } else if (srvHostname.startsWith(DnsSrv.DEFAULT_DNS_SECURE_SERVICE)) {
          srvHostname = srvHostname.replace(DnsSrv.DEFAULT_DNS_SECURE_SERVICE, "");
        }

        final List<String> foundNodes = fromDnsSrvOrThrowIfTlsRequired(srvHostname, tlsEnabled);
        if (foundNodes.isEmpty()) {
          throw new IllegalStateException("The loaded DNS SRV list from " + srvHostname + " is empty!");
        }
        Duration took = start.elapsed();
        eventBus.publish(new DnsSrvRecordsLoadedEvent(took, foundNodes));
        return foundNodes.stream().map(SeedNode::create).collect(Collectors.toSet());

      } catch (InvalidArgumentException t) {
        throw t;

      } catch (Throwable t) {
        Duration took = start.elapsed();
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
   * Assumes untyped ports are KV ports.
   * <p>
   * If a host appears in the connection string multiple times, all its ports are merged.
   * For example: {@code "couchbases://foo:123,foo:456=manager"} yields a single node
   * with KV port 123 and Manager port 456. If the cluster actually has multiple nodes
   * on the same host, some are ignored. While not ideal, this is good enough for bootstrapping.
   *
   * @param connectionString the instantiated connection string.
   * @return the set of seed nodes extracted.
   */
  static Set<SeedNode> populateSeedsFromConnectionString(final ConnectionString connectionString) {
    Map<String, List<UnresolvedSocket>> groupedByHost = connectionString.hosts().stream()
        .collect(groupingBy(UnresolvedSocket::host));

    Set<SeedNode> seedNodes = new HashSet<>();

    groupedByHost.forEach((host, addresses) -> {
      Map<PortType, Integer> ports = new EnumMap<>(PortType.class);
      PortType assumedPortType = connectionString.scheme() == COUCHBASE2 ? PortType.PROTOSTELLAR : PortType.KV;
      addresses.stream()
        .filter(it -> it.port() != 0)
        .forEach(it -> {
          if (connectionString.scheme() == COUCHBASE2 && (it.portType().isPresent() && it.portType().get() != PROTOSTELLAR)) {
            throw InvalidArgumentException.fromMessage("Invalid connection string. Port type " + it.portType().get() + " is not compatible with scheme " + connectionString.scheme());
          }

          ports.put(it.portType().orElse(assumedPortType), it.port());
        });

      seedNodes.add(SeedNode.create(host)
          .withKvPort(ports.get(KV))
          .withManagerPort(ports.get(MANAGER))
          .withProtostellarPort(ports.get(PROTOSTELLAR))
      );
    });

    return seedNodes;
  }

  /**
   * Sanity check a connection string for common errors that can be caught early on.
   */
  public static void sanityCheckPorts(final ConnectionString cs) {
    if (cs.scheme() == COUCHBASE2) {
      return;
    }

    for (UnresolvedSocket address : cs.hosts()) {
      // Prohibit "example.com:8091", but allow "example.com:8091=kv" as an escape hatch.
      if ((address.port() == 8091 || address.port() == 18091) && (!address.portType().isPresent())) {
        String recommended = cs.original()
          .replace(":8091", "")
          .replace(":18091", "");
        throw new InvalidArgumentException("Specifying 8091 or 18091 in the connection string \"" + cs.original() + "\" is " +
          "likely not what you want (it would connect to key/value via the management port which does not work). Please omit " +
          "the port and use \"" + recommended + "\" instead." , null, null);
      }
    }
  }

  /**
   * Returns true if the addresses indicate this is a Couchbase Capella cluster.
   */
  public static boolean isCapella(ConnectionString connectionString) {
    return connectionString.hosts().stream()
        .allMatch(it -> it.host().endsWith(".cloud.couchbase.com"));
  }

  /**
   * Returns a synthetic connection string corresponding to the seed nodes.
   */
  public static ConnectionString asConnectionString(Collection<SeedNode> nodes) {
    boolean hasProtostellarPort = nodes.stream().anyMatch(it -> it.protostellarPort().isPresent());
    boolean hasClassicPort = nodes.stream().anyMatch(it -> !it.protostellarPort().isPresent());
    if (hasClassicPort && hasProtostellarPort) {
      throw InvalidArgumentException.fromMessage("The seed nodes have an invalid combination of port types. Must be all Protostellar or all KV/Manager.");
    }

    List<String> addresses = new ArrayList<>();

    for (SeedNode node : nodes) {
      if (node.protostellarPort().isPresent()) {
        addresses.add(new HostAndPort(node.address(), node.protostellarPort().get()).format());
        continue;
      }

      if (!node.kvPort().isPresent() && !node.clusterManagerPort().isPresent()) {
        // Seed node did not specify any port.
        addresses.add(new HostAndPort(node.address(), 0).format());
        continue;
      }

      // Node has one or both of KV and Manager ports. If both, repeat the host
      // and let populateSeedsFromConnectionString reunify the "split" seed node later.
      node.kvPort().ifPresent(port -> addresses.add(new HostAndPort(node.address(), port).format() + "=kv"));
      node.clusterManagerPort().ifPresent(port -> addresses.add(new HostAndPort(node.address(), port).format() + "=manager"));
    }

    return ConnectionString.create(String.join(",", addresses))
      .withScheme(hasProtostellarPort ? COUCHBASE2 : COUCHBASE);
  }

  public static final String INCOMPATIBLE_CONNECTION_STRING_SCHEME =
    "Connection string scheme indicates a secure connection," +
      " but the pre-built ClusterEnvironment was not configured for TLS.";

  public static final String INCOMPATIBLE_CONNECTION_STRING_PARAMS =
    "Can't use a pre-built ClusterEnvironment with a connection string that has parameters.";

  public static void checkConnectionString(CoreEnvironment env, boolean ownsEnvironment, ConnectionString connStr) {
    boolean tls = env.securityConfig().tlsEnabled();

    if (!ownsEnvironment) {
      if (!tls && connStr.scheme() == COUCHBASES) {
        throw new IllegalArgumentException(INCOMPATIBLE_CONNECTION_STRING_SCHEME);
      }
      if (!tls && connStr.scheme() == COUCHBASE2) {
        throw new IllegalArgumentException(INCOMPATIBLE_CONNECTION_STRING_SCHEME);
      }
      if (!connStr.params().isEmpty()) {
        throw new IllegalArgumentException(INCOMPATIBLE_CONNECTION_STRING_PARAMS);
      }
    }

    boolean capella = isCapella(connStr);

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

  @Stability.Internal
  public static List<String> fromDnsSrvOrThrowIfTlsRequired(final String serviceName, boolean secure) throws NamingException {
    final boolean full = false;

    // If the user enabled TLS, just return records for the secure protocol.
    if (secure) {
      return DnsSrv.fromDnsSrv(serviceName, full, true);
    }

    try {
      // User didn't enable TLS, so try to return records for the insecure protocol.
      return DnsSrv.fromDnsSrv(serviceName, full, false);

    } catch (NameNotFoundException errorFromFirstLookup) {
      // There's no DNS SRV record for the insecure protocol.
      // If there's one for the secure protocol, tell the user TLS is required.
      try {
        if (!DnsSrv.fromDnsSrv(serviceName, full, true).isEmpty()) {
          throw InvalidArgumentException.fromMessage(
              "The DNS SRV record for '" + redactSystem(serviceName) + "' indicates" +
                  " TLS must be used when connecting to this cluster." +
                  " Please enable TLS by setting the 'security.enableTls' client setting to true." +
                  " If the Cluster does not use a shared ClusterEnvironment, an alternative way to enable TLS is to" +
                  " prefix the connection string with \"couchbases://\" (note the final 's')");
        }

        throw errorFromFirstLookup;

      } catch (InvalidArgumentException propagateMe) {
        throw propagateMe;

      } catch (Exception e) {
        if (e != errorFromFirstLookup) {
          errorFromFirstLookup.addSuppressed(e);
        }
        throw errorFromFirstLookup;
      }
    }
  }
}
