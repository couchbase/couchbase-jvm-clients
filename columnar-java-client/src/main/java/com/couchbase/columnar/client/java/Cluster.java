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
import com.couchbase.client.core.env.InvalidPropertyException;
import com.couchbase.client.core.transaction.atr.ActiveTransactionRecordIds;
import com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig;
import com.couchbase.client.core.transaction.config.CoreTransactionsConfig;
import com.couchbase.client.core.transaction.forwards.CoreTransactionsSupportedExtensions;
import com.couchbase.client.core.util.ConnectionString;
import com.couchbase.columnar.client.java.internal.Certificates;
import com.couchbase.columnar.client.java.internal.ThreadSafe;
import reactor.core.publisher.Mono;

import javax.net.ssl.TrustManagerFactory;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.transaction.config.CoreTransactionsCleanupConfig.DEFAULT_TRANSACTION_CLEANUP_WINDOW;
import static com.couchbase.client.core.transaction.config.CoreTransactionsConfig.DEFAULT_TRANSACTION_DURABILITY_LEVEL;
import static com.couchbase.client.core.transaction.config.CoreTransactionsConfig.DEFAULT_TRANSACTION_TIMEOUT;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

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
 * For best efficiency, create a single `Cluster` instance
 * per Columnar cluster and share it throughout your application.
 * <p>
 * When you're done interacting with the cluster, it's important to call
 * {@link Cluster#close()} to release resources used by the cluster.
 */
@ThreadSafe
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
   * <p>
   * Example usage:
   * <pre>
   * Cluster cluster = Cluster.newInstance(
   *     connectionString,
   *     Credential.of(username, password),
   *     options -> options
   *         .timeout(it -> it.queryTimeout(Duration.ofMinutes(5)))
   *         .deserializer(new JacksonDeserializer(new ObjectMapper()))
   * );
   * </pre>
   *
   * @see Credential#of(String, String)
   * @see #newInstance(String, Credential)
   * @see ClusterOptions
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

    applyConnectionStringParameters(builder, cs);

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

  private static void applyConnectionStringParameters(ClusterOptions builder, ConnectionString cs) {
    // Make a mutable copy so we can remove entries that require special handling.
    LinkedHashMap<String, String> params = new LinkedHashMap<>(cs.params());

    // "security.trust_only_non_prod" is special; it doesn't have a corresponding programmatic
    // config option. It's not a secret, but we don't want to confuse external users with a
    // security config option they never need to set.
    boolean trustOnlyNonProdCertificates = lastTrustParamIsNonProd(params);

    try {
      BuilderPropertySetter propertySetter = new BuilderPropertySetter("", Collections.emptyMap(), Cluster::lowerSnakeCaseToLowerCamelCase);
      propertySetter.set(builder, params);

    } catch (InvalidPropertyException e) {
      // Translate core-io exception (internal API) to platform exception!
      throw new IllegalArgumentException(e.getMessage(), e.getCause());
    }

    // Do this last, after any other "trust_only_*" params are validated and applied.
    // Otherwise, the earlier params would clobber the config set by this param.
    // (There's no compelling use case for including multiple "trust_only_*" params in
    // the connection string, but we behave consistently if someone tries it.)
    if (trustOnlyNonProdCertificates) {
      builder.security(it -> it.trustOnlyCertificates(Certificates.getNonProdCertificates()));
    }
  }

  /**
   * Returns true if the "security.trust_only_non_prod" connection string param is
   * present, and no other trust params appear after it (since last one wins).
   * <p>
   * Side effect: Removes that param from the map.
   *
   * @throws IllegalArgumentException if the param has an invalid value
   */
  private static boolean lastTrustParamIsNonProd(LinkedHashMap<String, String> params) {
    final String TRUST_ONLY_NON_PROD_PARAM = "security.trust_only_non_prod";

    // Last trust param wins, so check whether "trust only non-prod" was last trust param.
    boolean trustOnlyNonProdWasLast = params.keySet().stream()
      .filter(it -> it.startsWith("security.trust_"))
      .reduce((a, b) -> b) // last
      .orElse("")
      .equals(TRUST_ONLY_NON_PROD_PARAM);

    // Always remove it, so later processing doesn't treat it as unrecognized param.
    String trustOnlyNonProdValue = params.remove(TRUST_ONLY_NON_PROD_PARAM);

    // Always validate if present, regardless of whether it was last.
    if (trustOnlyNonProdValue != null && !Set.of("", "true", "1").contains(trustOnlyNonProdValue)) {
      throw new IllegalArgumentException("Invalid value for connection string property '" + TRUST_ONLY_NON_PROD_PARAM + "'; expected 'true', '1', or empty string, but got: '" + trustOnlyNonProdValue + "'");
    }

    return trustOnlyNonProdWasLast;
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

  /**
   * Releases resources and prevents further use of this object.
   */
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

  /**
   * Returns the database in this cluster with the given name.
   * <p>
   * A database is a container for {@link Scope}s.
   * <p>
   * If the database does not exist, this method still returns a
   * non-null object, but operations using that object fail with
   * an exception indicating the database does not exist.
   */
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
