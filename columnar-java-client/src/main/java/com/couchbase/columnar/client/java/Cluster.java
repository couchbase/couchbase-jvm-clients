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

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.BuilderPropertySetter;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;
import com.couchbase.client.core.util.ConnectionString;
import reactor.core.publisher.Mono;

import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig.DEFAULT_TRANSACTION_CLEANUP_WINDOW;
import static com.couchbase.client.core.transaction.config.CoreTransactionsConfig.DEFAULT_TRANSACTION_DURABILITY_LEVEL;
import static com.couchbase.client.core.transaction.config.CoreTransactionsConfig.DEFAULT_TRANSACTION_TIMEOUT;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Create a new instance by calling {@link #newInstance}.
 * <p>
 * To create an instance with default options:
 * <pre>
 * Cluster cluster = Cluster.newInstance(
 *     connectionString,
 *     Credential.of(username, password)
 * );
 * </pre>
 * To create an instance with custom options:
 * <pre>
 * Cluster cluster = Cluster.newInstance(
 *     connectionString,
 *     Credential.of(username, password),
 *     options -> options
 *         .timeout(it -> it.queryTimeout(Duration.ofMinutes(5)))
 *         .deserializer(new JacksonDeserializer(new ObjectMapper()))
 * );
 * </pre>
 */
public final class Cluster implements Closeable, Queryable {
  private final Environment environment;
  private final CoreCouchbaseOps couchbaseOps;

  final QueryExecutor queryExecutor;

  /**
   * Remembers whether {@link #disconnectInternal} was called.
   */
  private final AtomicBoolean disconnected = new AtomicBoolean();

  /**
   * Returns a new instance, with default options.
   * <p>
   * Example usage:
   * <pre>
   * Cluster cluster = Cluster.newInstance(
   *     "couchbases://example.com",
   *     Credentials.of(username, password)
   * );
   * </pre>
   *
   * @see #newInstance(String, Credential, Consumer)
   * @see Credential#of(String, String)
   */
  public static Cluster newInstance(
    String connectionString,
    Credential credential
  ) {
    return newInstance(
      connectionString,
      credential,
      options -> {
      }
    );
  }

  /**
   * Returns a new instance, with options customized by the {@code optionsCustomizer} callback.
   *
   * @see Credential#of(String, String)
   * @see #newInstance(String, Credential)
   */
  public static Cluster newInstance(
    String connectionString,
    Credential credential,
    Consumer<ClusterOptions> optionsCustomizer
  ) {
    ConnectionString cs = ConnectionString.create(connectionString);

    if (cs.scheme() != ConnectionString.Scheme.COUCHBASES) {
      throw new IllegalArgumentException("Invalid connection string; must start with secure scheme \"couchbases://\" (note the final 's') but got: " + redactUser(cs.original()));
    }

    checkParameterNamesAreLowercase(cs);

    ClusterOptions builder = new ClusterOptions();
    optionsCustomizer.accept(builder);

    BuilderPropertySetter propertySetter = new BuilderPropertySetter("", Collections.emptyMap(), Cluster::lowerSnakeCaseToLowerCamelCase);
    propertySetter.set(builder, cs.params());

    // do we really want to allow a system property to disable server certificate verification?
    //propertySetter.set(builder, systemPropertyMap(SYSTEM_PROPERTY_PREFIX));

    ClusterOptions.Unmodifiable opts = builder.build();

    Environment.Builder envBuilder = new Environment.Builder()
      .transactionsConfig(disableTransactionsCleanup())
      .deserializer(opts.deserializer())
      .ioConfig(it -> it
        .enableDnsSrv(opts.srv())
        .maxHttpConnections(Integer.MAX_VALUE)
      )
      .securityConfig(it -> {
          SecurityOptions.Unmodifiable security = opts.security();

          it.enableTls(true);

          if (!security.cipherSuites().isEmpty()) {
            it.ciphers(security.cipherSuites());
          }

          TrustManagerFactory factory = security.trustSource().trustManagerFactory();
          if (factory != null) {
            it.trustManagerFactory(factory);
          } else {
            it.trustCertificates(security.trustSource().certificates());
          }
        }
      );

    TimeoutOptions.Unmodifiable timeouts = opts.timeout();
    envBuilder.timeoutConfig(it -> it
      .connectTimeout(timeouts.connectTimeout())
      .analyticsTimeout(timeouts.queryTimeout())
    );

    // Not exposing config options for native I/O, since it's
    // unclear whether future SDK versions will retain this feature.
    // Disable it to avoid exploding on Alpine Linux where there's no glibc.
    envBuilder
      .ioEnvironment(it -> it.enableNativeIo(false))
      .securityConfig(it -> it.enableNativeTls(false));

    Environment env = envBuilder.build();

    return new Cluster(cs, credential.toInternalAuthenticator(), env);
  }

  private static void checkParameterNamesAreLowercase(ConnectionString cs) {
    cs.params().keySet().stream()
      .filter(Cluster::hasUppercase)
      .findFirst()
      .ifPresent(badName -> {
        throw new IllegalArgumentException("Invalid connection string parameter '" + badName + "'. Please use lower_snake_case in connection string parameter names.");
      });
  }

  private static boolean hasUppercase(String s) {
    return s.codePoints().anyMatch(Character::isUpperCase);
  }

  private static String lowerSnakeCaseToLowerCamelCase(String s) {
    StringBuilder sb = new StringBuilder();
    int[] codePoints = s.codePoints().toArray();

    boolean prevWasUnderscore = false;
    for (int i : codePoints) {
      if (i == '_') {
        prevWasUnderscore = true;
        continue;
      }

      if (prevWasUnderscore) {
        i = Character.toUpperCase(i);
      }
      sb.appendCodePoint(i);
      prevWasUnderscore = false;
    }

    return sb.toString();
  }

  private static CoreTransactionsConfig disableTransactionsCleanup() {
    return new CoreTransactionsConfig(
      DEFAULT_TRANSACTION_DURABILITY_LEVEL,
      DEFAULT_TRANSACTION_TIMEOUT,
      new CoreTransactionsCleanupConfig(false, false, DEFAULT_TRANSACTION_CLEANUP_WINDOW, emptySet()),
      null,
      null,
      null,
      ActiveTransactionRecordIds.NUM_ATRS_DEFAULT,
      Optional.empty(),
      Optional.empty(),
      CoreTransactionsSupportedExtensions.NONE
    );
  }

  private static final String SYSTEM_PROPERTY_PREFIX = "com.couchbase.columnar.env.";

  /**
   * Returns a map of all system properties whose names start with the given prefix,
   * transformed to remove the prefix.
   */
  private static Map<String, String> systemPropertyMap(String prefix) {
    return System.getProperties()
      .entrySet()
      .stream()
      .filter(entry -> entry.getKey() instanceof String && entry.getValue() instanceof String)
      .filter(entry -> ((String) entry.getKey()).startsWith(prefix))
      .collect(toMap(e -> ((String) e.getKey()).substring(prefix.length()), e -> (String) e.getValue()));
  }

  /**
   * @see #newInstance
   */
  private Cluster(
    ConnectionString connectionString,
    Authenticator authenticator,
    Environment environment
  ) {
    this.environment = requireNonNull(environment);
    this.couchbaseOps = CoreCouchbaseOps.create(environment, authenticator, connectionString);

    Core core = couchbaseOps.asCore();
    core.initGlobalConfig();

    this.queryExecutor = new QueryExecutor(core, environment, connectionString);
  }

  public void close() {
    Duration timeout = environment.timeoutConfig().disconnectTimeout();
    disconnectInternal(disconnected, timeout, couchbaseOps, environment).block();
  }

  static Mono<Void> disconnectInternal(
    final AtomicBoolean disconnected,
    final Duration timeout,
    final CoreCouchbaseOps couchbaseOps,
    final CoreEnvironment environment
  ) {
    return couchbaseOps.shutdown(timeout)
      .then(environment.shutdownReactive(timeout))
      .then(Mono.fromRunnable(() -> disconnected.set(true)));
  }

  public Database database(String name) {
    return new Database(this, name);
  }

  @Override
  public QueryResult executeQuery(
    String statement,
    Consumer<QueryOptions> optionsCustomizer
  ) {
    return queryExecutor.queryBuffered(statement, optionsCustomizer, null);
  }

  @Override
  public QueryMetadata executeStreamingQuery(
    String statement,
    Consumer<Row> rowAction,
    Consumer<QueryOptions> optionsCustomizer
  ) {
    return queryExecutor.queryStreaming(statement, optionsCustomizer, null, rowAction);
  }
}
