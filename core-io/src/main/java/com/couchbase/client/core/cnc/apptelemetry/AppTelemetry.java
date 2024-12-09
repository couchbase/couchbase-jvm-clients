/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.apptelemetry;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollectorImpl;
import com.couchbase.client.core.cnc.apptelemetry.reporter.AppTelemetryReporter;
import com.couchbase.client.core.cnc.apptelemetry.reporter.AppTelemetryReporterImpl;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.topology.ClusterTopology;
import com.couchbase.client.core.topology.HostAndServicePorts;
import com.couchbase.client.core.util.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.annotation.Nullable;

import java.io.Closeable;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class AppTelemetry implements Closeable {
  public final AppTelemetryCollector collector;
  public final AppTelemetryReporter reporter;

  private AppTelemetry(AppTelemetryCollector collector, AppTelemetryReporter reporter) {
    this.collector = requireNonNull(collector);
    this.reporter = requireNonNull(reporter);
  }

  public static AppTelemetry from(CoreContext ctx) {
    CoreEnvironment env = ctx.environment();

    if (env.appTelemetryDisabled()) {
      return new AppTelemetry(
        AppTelemetryCollector.NOOP,
        AppTelemetryReporter.NOOP
      );
    }

    ConfigurationProvider configurationProvider = ctx.core().configurationProvider();

    AppTelemetryCollector collector = new AppTelemetryCollectorImpl(
      configurationProvider.configs(),
      env.userAgent()
    );

    AppTelemetryReporter reporter = new AppTelemetryReporterImpl(ctx, collector);

    URI appTelemetryUri = env.appTelemetryEndpoint();
    if (appTelemetryUri != null) {
      reporter.updateRemotes(setOf(appTelemetryUri));
    } else {
      boolean tls = env.securityConfig().tlsEnabled();
      appTelemetryUris(tls, configurationProvider)
        .doOnNext(reporter::updateRemotes)
        .subscribe();
    }

    return new AppTelemetry(collector, reporter);
  }

  private static Flux<Set<URI>> appTelemetryUris(
    boolean tls,
    ConfigurationProvider configurationProvider
  ) {
    return configurationProvider.configs()
      .map(clusterconfig -> allNodes(clusterconfig)
        .map(node -> {
          String path = node.appTelemetryPath();
          if (path == null) return null;

          int managerPort = node.port(ServiceType.MANAGER).orElseThrow(() -> new NoSuchElementException("missing manager port?"));
          HostAndPort managerAddress = new HostAndPort(node.host(), managerPort);
          String scheme = tls ? "wss" : "ws";
          return URI.create(scheme + "://" + managerAddress.format() + path);
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toSet())
      );
  }

  @Override
  public void close() {
    reporter.close();
  }

  private static Stream<HostAndServicePorts> allNodes(ClusterConfig config) {
    return allTopologies(config)
      .flatMap(it -> it.nodes().stream());
  }

  private static Stream<ClusterTopology> allTopologies(ClusterConfig config) {
    return Stream.concat(
      streamOfNullable(config.globalTopology()),
      config.bucketTopologies().stream()
    );
  }

  private static <T> Stream<T> streamOfNullable(@Nullable T item) {
    return item == null ? Stream.empty() : Stream.of(item);
  }
}
