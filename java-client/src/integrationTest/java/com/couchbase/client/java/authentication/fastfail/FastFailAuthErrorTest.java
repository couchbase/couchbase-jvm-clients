/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.java.authentication.fastfail;

import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.diagnostics.AuthenticationStatus;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions;
import com.couchbase.client.java.manager.user.AuthDomain;
import com.couchbase.client.java.manager.user.User;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * If bad credentials have been passed, an AUTHENTICATION_ERROR RetryReason should be raised internally, which a custom
 * RetryStrategy can use to fast fail with an AuthenticationFailureException.
 * <p>
 * Tested:
 * - couchbase:// couchbases:// and protostellar://
 * - bad credentials vs bucket not being there vs cluster being down vs bad RBAC
 * - various operations (KV, waitUntilReady, query, etc.)
 * - AuthenticationStatus in EndpointDiagnostic is correct
 */
@IgnoreWhen(clusterTypes = {com.couchbase.client.test.ClusterType.MOCKED}, missesCapabilities = {Capabilities.COLLECTIONS})
class FastFailAuthErrorTest extends JavaIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastFailAuthErrorTest.class);

  /**
   * Abstracts over performing KV, query etc. operations.
   */
  interface Operation {

    default void execute(Bucket bucket) {
      execute(bucket, null);
    }

    void execute(Bucket bucket, @Nullable RetryStrategy customRetryStrategy);

    String name();

    OpType opType();
  }

  static class KVOperation implements Operation {
    @Override
    public void execute(Bucket bucket, @Nullable RetryStrategy customRetryStrategy) {
      Collection collection = bucket.defaultCollection();
      InsertOptions options = InsertOptions.insertOptions();
      if (customRetryStrategy != null) {
        options.retryStrategy(customRetryStrategy);
      }
      collection.insert(UUID.randomUUID().toString(), "Hello, World", options);
    }

    @Override
    public String name() {
      return "kv";
    }

    @Override
    public OpType opType() {
      return OpType.REQUIRES_BUCKET_CONNECTION;
    }
  }

  // Testing KV, Query and QueryIndexManager should test all paths.
  static class QueryOperation implements Operation {
    @Override
    public void execute(Bucket bucket, @Nullable RetryStrategy customRetryStrategy) {
      QueryOptions options = QueryOptions.queryOptions();
      if (customRetryStrategy != null) {
        options.retryStrategy(customRetryStrategy);
      }
      bucket.defaultScope().query("SELECT 'Hello World' AS Greeting", options);
    }

    @Override
    public String name() {
      return "query";
    }

    @Override
    public OpType opType() {
      return OpType.DOES_NOT_REQUIRE_BUCKET_CONNECTION;
    }
  }

  class QueryIndexManager implements Operation {
    @Override
    public void execute(Bucket bucket, @Nullable RetryStrategy customRetryStrategy) {
      GetAllQueryIndexesOptions options = GetAllQueryIndexesOptions.getAllQueryIndexesOptions();
      if (customRetryStrategy != null) {
        options.retryStrategy(customRetryStrategy);
      }
      bucket.defaultCollection().queryIndexes().getAllIndexes(options);
    }

    @Override
    public String name() {
      return "query-index-manager";
    }

    @Override
    public OpType opType() {
      return OpType.DOES_NOT_REQUIRE_BUCKET_CONNECTION;
    }
  }

  /**
   * Abstracts over the various flavours of clusters (and Clusters).
   *
   * A custom retry strategy has been applied on all Clusters except AVAILABLE_BAD_CREDENTIALS_NO_CUSTOM_RETRY_STRAT.
   */
  enum ClusterType {
    /**
     * A cluster that can be reached but where bad credentials have been passed (user exists, password is wrong).  Should trigger AUTHENTICATION_ERROR.
     */
    AVAILABLE_BAD_CREDENTIALS,

    /**
     * A cluster that can be reached but where bad credentials have been passed (user does not exist).  Should trigger AUTHENTICATION_ERROR.
     */
    AVAILABLE_USER_DOES_NOT_EXIST,

    /**
     * A cluster that can be reached but where bad credentials have been passed.  Should trigger AUTHENTICATION_ERROR.
     * <p>
     * The only difference from AVAILABLE_BAD_CREDENTIALS is that it doesn't have a custom retry strategy (so we can test that at the per-op level).
     */
    AVAILABLE_BAD_CREDENTIALS_NO_CUSTOM_RETRY_STRAT,

    /**
     * A cluster that can be reached and where good credentials have been passed and the user has good RBACs.  Should not trigger AUTHENTICATION_ERROR.
     */
    AVAILABLE_GOOD_CREDENTIALS,

    /**
     * A cluster that can be reached and where good credentials have been passed, but the user does not have RBAC to do anything.  Should not trigger AUTHENTICATION_ERROR.
     */
    AVAILABLE_GOOD_CREDENTIALS_BAD_RBAC,

    /**
     * A cluster that cannot be reached.  Should not be capable of triggering AUTHENTICATION_ERROR.  (Whether the credentials are good or bad doesn't matter).
     */
    UNAVAILABLE
  }

  enum OpType {
    REQUIRES_BUCKET_CONNECTION,

    DOES_NOT_REQUIRE_BUCKET_CONNECTION
  }

  static class ClusterAndDesc {
    public final Cluster cluster;
    public final ClusterType type;
    public final String desc;

    ClusterAndDesc(Cluster cluster, ClusterType type, String desc) {
      this.cluster = cluster;
      this.type = type;
      this.desc = desc;
    }
  }

  private static final List<ClusterAndDesc> clusters = new ArrayList<>();

  @BeforeAll
  public static void beforeAll() {
    String badUsername = "badname";
    String badPassword = "badpass";
    String goodUsername = config().adminUsername();
    String goodPassword = config().adminPassword();
    String goodHostname = config().nodes().get(0).hostname();

    ClusterAndDesc goodCluster = createCluster("couchbase://" + goodHostname, goodUsername, goodPassword, ClusterType.AVAILABLE_GOOD_CREDENTIALS);
    goodCluster.cluster.waitUntilReady(Duration.ofSeconds(30));
    clusters.add(goodCluster);

    // Create a user that has no permissions to do anything
    User user = new User("no_perms");
    user.password(goodPassword);
    goodCluster.cluster.users().upsertUser(user);
    ConsistencyUtil.waitUntilUserPresent(goodCluster.cluster.core(), AuthDomain.LOCAL.alias(), user.username());

    clusters.add(createCluster("couchbases://" + goodHostname, goodUsername, goodPassword, ClusterType.AVAILABLE_GOOD_CREDENTIALS));

    clusters.add(createCluster("couchbase://" + goodHostname, goodUsername, badPassword, ClusterType.AVAILABLE_BAD_CREDENTIALS));
    clusters.add(createCluster("couchbases://" + goodHostname, goodUsername, badPassword, ClusterType.AVAILABLE_BAD_CREDENTIALS));

    clusters.add(createCluster("couchbase://" + goodHostname, badUsername, badPassword, ClusterType.AVAILABLE_USER_DOES_NOT_EXIST));
    clusters.add(createCluster("couchbases://" + goodHostname, badUsername, badPassword, ClusterType.AVAILABLE_USER_DOES_NOT_EXIST));

    clusters.add(createCluster("couchbase://" + goodHostname, goodUsername, badPassword, ClusterType.AVAILABLE_BAD_CREDENTIALS_NO_CUSTOM_RETRY_STRAT, false));
    clusters.add(createCluster("couchbases://" + goodHostname, goodUsername, badPassword, ClusterType.AVAILABLE_BAD_CREDENTIALS_NO_CUSTOM_RETRY_STRAT, false));

    clusters.add(createCluster("couchbase://unreachable", goodUsername, goodPassword, ClusterType.UNAVAILABLE));
    clusters.add(createCluster("couchbases://unreachable", goodUsername, goodPassword, ClusterType.UNAVAILABLE));

    clusters.add(createCluster("couchbase://" + goodHostname, user.username(), goodPassword, ClusterType.AVAILABLE_GOOD_CREDENTIALS_BAD_RBAC));
    clusters.add(createCluster("couchbases://" + goodHostname, user.username(), goodPassword, ClusterType.AVAILABLE_GOOD_CREDENTIALS_BAD_RBAC));


    config().nodes().get(0).protostellarPort().ifPresent(protostellarPort -> {
      clusters.add(createCluster("protostellar://" + goodHostname + ":" + protostellarPort, badUsername, badPassword, ClusterType.AVAILABLE_BAD_CREDENTIALS));
      clusters.add(createCluster("protostellar://" + goodHostname + ":" + protostellarPort, goodUsername, goodPassword, ClusterType.AVAILABLE_BAD_CREDENTIALS));
      clusters.add(createCluster("protostellar://unreachable", badUsername, badPassword, ClusterType.UNAVAILABLE));
    });
  }

  @AfterAll
  public static void afterAll() {
    clusters.forEach(cluster -> cluster.cluster.disconnect());
  }

  private static ClusterAndDesc createCluster(String hostname, String username, String password, ClusterType type) {
    return createCluster(hostname, username, password, type, true);
  }

  private static ClusterAndDesc createCluster(String connectionString, String username, String password, ClusterType type, boolean useCustomRetryStrategy) {
    String clusterDesc = String.format("%s %s:%s", connectionString, username, password);

    LOGGER.info("Creating cluster {}", clusterDesc);

    Cluster cluster = Cluster.connect(connectionString,
      ClusterOptions.clusterOptions(username, password)
        .environment(env -> env.retryStrategy(useCustomRetryStrategy ? FastFailOnAuthErrorRetryStrategy.INSTANCE : BestEffortRetryStrategy.INSTANCE)
          .transactionsConfig(TransactionsConfig.cleanupConfig(TransactionsCleanupConfig.builder().cleanupLostAttempts(false).cleanupClientAttempts(false)))
          .securityConfig(cfg -> cfg.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE))
          // We spend a lot of time waiting for timeouts in these tests, so keep it short
          .timeoutConfig(cfg ->
            cfg.kvTimeout(Duration.ofSeconds(1))
              .queryTimeout(Duration.ofSeconds(1)))));

    return new ClusterAndDesc(cluster, type, clusterDesc);
  }

  static class TestParams {
    public final ClusterAndDesc cluster;

    TestParams(ClusterAndDesc cluster) {
      this.cluster = cluster;
    }

    @Override
    public String toString() {
      return String.format("cluster=" + cluster.desc);
    }
  }

  static class TestParamsWithOp extends TestParams {
    public final Operation operation;

    TestParamsWithOp(Operation operation, ClusterAndDesc cluster) {
      super(cluster);
      this.operation = operation;
    }

    @Override
    public String toString() {
      return String.format("op=%s %s", operation.name(), super.toString());
    }
  }

  private Stream<Operation> operations() {
    return Stream.of(new KVOperation(), new QueryOperation(), new QueryIndexManager());
  }

  private Stream<TestParams> iterateInternal(ClusterType type) {
    return clusters.stream()
      .filter(cluster -> cluster.type == type)
      .map(TestParams::new);
  }

  private Stream<DynamicTest> iterate(ClusterType type, Consumer<TestParams> test) {
    return iterateInternal(type).map(params ->
      DynamicTest.dynamicTest(params.toString(), () -> {
        test.accept(params);
      }));
  }

  private Stream<DynamicTest> iterateWithOp(ClusterType type, @Nullable OpType opType, Consumer<TestParamsWithOp> test) {
    return iterateInternal(type)
      .flatMap(params -> operations()
        .filter(op -> opType == null || op.opType() == opType)
        .map(op -> {
          TestParamsWithOp paramsWithOp = new TestParamsWithOp(op, params.cluster);

          return DynamicTest.dynamicTest(paramsWithOp.toString(), () -> {
            test.accept(paramsWithOp);
          });
        }));
  }

  /**
   * The 'main' test - if bad credentials are used, AUTHENTICATION_ERROR should be raisable on all ops.
   * <p>
   * Note that waitUntilReady now always raises AuthenticationFailureException on AUTHENTICATION_ERROR, as it doesn't look at the
   * RetryStrategy (unless it gets to the ping stage, which it won't on AUTHENTICATION_ERROR).
   */
  @TestFactory
  Stream<DynamicTest> operationFailsWithAuthErrorOnBadCredentials() {
    return iterateWithOp(ClusterType.AVAILABLE_BAD_CREDENTIALS, null, p -> {
      Bucket bucket = p.cluster.cluster.bucket(config().bucketname());
      assertThrows(AuthenticationFailureException.class, () -> p.operation.execute(bucket));
    });
  }

  /**
   * Same test except with an op-level custom RetryStrategy.
   */
  @TestFactory
  Stream<DynamicTest> operationFailsWithAuthErrorOnBadCredentialsOpLevelCustomRetryStrategy() {
    return iterateWithOp(ClusterType.AVAILABLE_BAD_CREDENTIALS, null, p -> {
      Bucket bucket = p.cluster.cluster.bucket(config().bucketname());
      int authErrorsRequired = 3;
      FastFailOnNthAuthErrorRetryStrategy retryStrategy = new FastFailOnNthAuthErrorRetryStrategy(authErrorsRequired);
      assertThrows(AuthenticationFailureException.class, () -> p.operation.execute(bucket, retryStrategy));
      assertEquals(authErrorsRequired, retryStrategy.authErrors());
    });
  }

  /**
   * If the user doesn't exist, should get AUTHENTICATION_ERROR on all ops.
   */
  @TestFactory
  Stream<DynamicTest> operationFailsWithAuthErrorWhenUserDoesNotExist() {
    return iterateWithOp(ClusterType.AVAILABLE_USER_DOES_NOT_EXIST, null, p -> {
      Bucket bucket = p.cluster.cluster.bucket(config().bucketname());
      assertThrows(AuthenticationFailureException.class, () -> p.operation.execute(bucket));
    });
  }

  /**
   * If the cluster is unavailable, all operations should timeout rather than raise an AUTHENTICATION_ERROR.
   */
  @TestFactory
  Stream<DynamicTest> operationsTimeoutWhenClusterUnavailable() {
    return iterateWithOp(ClusterType.UNAVAILABLE, null, p -> {
      Bucket bucket = p.cluster.cluster.bucket(config().bucketname());
      assertThrows(TimeoutException.class, () -> p.operation.execute(bucket));
    });
  }

  /**
   * If valid credentials are passed but the bucket does not exist, it shouldn't raise AUTHENTICATION_ERROR.
   */
  @TestFactory
  Stream<DynamicTest> operationsTimeoutWhenBucketDoesNotExist() {
    return iterateWithOp(ClusterType.AVAILABLE_GOOD_CREDENTIALS, OpType.REQUIRES_BUCKET_CONNECTION, p -> {
      Bucket bucket = p.cluster.cluster.bucket("does_not_exist");
      assertThrows(TimeoutException.class, () -> p.operation.execute(bucket));
    });
  }

  /**
   * Test the new AuthenticationStatus part of EndpointDiagnostics is correct after a successful connection.
   */
  @TestFactory
  Stream<DynamicTest> ifOperationSucceedsAuthenticationStatusIsCorrect() {
    return iterateWithOp(ClusterType.AVAILABLE_GOOD_CREDENTIALS, null, p -> {
      Bucket bucket = p.cluster.cluster.bucket(config().bucketname());
      p.operation.execute(bucket);
    });
  }

  /**
   * If valid credentials are passed but the user doesn't have RBACs, it shouldn't raise AUTHENTICATION_ERROR.
   */
  @TestFactory
  Stream<DynamicTest> operationsTimeoutUserDoesNotHaveRBAC() {
    return iterateWithOp(ClusterType.AVAILABLE_GOOD_CREDENTIALS_BAD_RBAC, null, p -> {
      Bucket bucket = p.cluster.cluster.bucket(config().bucketname());
      assertThrows(TimeoutException.class, () -> p.operation.execute(bucket));
    });
  }
}
