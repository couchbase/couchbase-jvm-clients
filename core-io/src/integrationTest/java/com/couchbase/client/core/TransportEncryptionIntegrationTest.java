/*
 * Copyright 2015 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.couchbase.client.core;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.endpoint.EndpointConnectionFailedEvent;
import com.couchbase.client.core.cnc.events.io.SecureConnectionFailedEvent;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.Flaky;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 * Verifies that the core is able to connect and perform operations over encrypted
 * transports.
 *
 * <p>Note that since the mock does not support encrypted connections, they are ignored
 * on it.</p>
 */
@IgnoreWhen(clusterTypes = { ClusterType.MOCKED, ClusterType.CAVES },
  missesCapabilities = { Capabilities.ENTERPRISE_EDITION })
class TransportEncryptionIntegrationTest extends CoreIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(TransportEncryptionIntegrationTest.class);

  private static final Set<ServiceType> serviceTypes = new HashSet<>();
  static {
    serviceTypes.add(ServiceType.KV);
  }
  /**
   * Helper method to configure the secure environment based on the integration seed nodes
   * from the target cluster and the security config from each test.
   *
   * @param config the security config to use.
   * @return a core environment, set up for encrypted networking.
   */
  private CoreEnvironment secureEnvironment(final SecurityConfig.Builder config, EventBus customEventBus) {
    CoreEnvironment.Builder builder = environment().securityConfig(config);

    if (customEventBus != null) {
      builder.eventBus(customEventBus);
    }

    return builder.build();
  }

  private Set<SeedNode> secureSeeds() {
    return config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.ofNullable(cfg.ports().get(Services.KV_TLS)),
      Optional.ofNullable(cfg.ports().get(Services.MANAGER_TLS))
    )).collect(Collectors.toSet());
  }

  @Test
  void performsKeyValueIgnoringServerCert() throws Exception {
    CoreEnvironment env = secureEnvironment(SecurityConfig
      .enableTls(true)
      .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE), null);

    Core core = createCore(env, authenticator(), secureSeeds());

    try {
      runKeyValueOperation(core, env);
    } finally {
      core.shutdown().block();
      env.shutdown();
    }
  }

  @IgnoreWhen(missesCapabilities = {Capabilities.ENTERPRISE_EDITION})
  @Test
  void performsKeyValueWithServerCert() throws Exception {
    if (!config().clusterCerts().isPresent()) {
      fail("Cluster Certificate must be present for this test!");
    }
    
    try (
      CoreEnvironment env = secureEnvironment(
        SecurityConfig.enableTls(true).trustCertificates(config().clusterCerts().get()), null);
        Core core = createCore(env, authenticator(), secureSeeds())) {
          runKeyValueOperation(core, env);
        }
  }

  @Flaky
  @Test
  void allowsToConfigureCustomCipher() throws Exception {
    String version = System.getProperty("java.version");
    boolean is8 = version.startsWith("8") || version.startsWith("1.8");

    logger.info("Java version: {} {}", version, is8);

    // The logic below has gone through several iterations.  The original hardcoded cipher was removed in a particular build
    // of JDK 8.  Logic was added to work through the list of supported ciphers, but this hits various SSL errors ("insufficient_security",
    // "handshake_failure") and fails to find a cipher that works in a reasonable time.
    // The common denominator in the failures is JDK 8, so not running this test on that.
    // Update: the logic to find a supported cipher was still failing on some versions of JDK11+.  It appears to be intermittent so perhaps the list of supported ciphers is unorded.  Going back to hardcoded cipher.
    if (!is8) {
      if (!config().clusterCerts().isPresent()) {
        fail("Cluster Certificate must be present for this test!");
      }

      String cipher = "TLS_AES_256_GCM_SHA384";
      CoreEnvironment env = secureEnvironment(
        SecurityConfig.enableTls(true).ciphers(Collections.singletonList(cipher))
          .trustCertificates(config().clusterCerts().get()),
        null);
      try (Core core = createCore(env, authenticator(), secureSeeds())) {
        runKeyValueOperation(core, env);
      }
    }
  }

  @Test
  void loadsSecurityConfigFromTrustStore() throws Exception {
    if (!config().clusterCerts().isPresent()) {
      fail("Cluster Certificate must be present for this test!");
    }

    // Prepare a keystore and load it with the cert
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    trustStore.setCertificateEntry("server", config().clusterCerts().get().get(0));

    try (
      CoreEnvironment env = secureEnvironment(SecurityConfig.enableTls(true).trustStore(trustStore), null);
      Core core = createCore(env, authenticator(), secureSeeds())) {
        runKeyValueOperation(core, env);
    }

  }

  private void runKeyValueOperation(Core core, CoreEnvironment env) throws Exception {
    String id = UUID.randomUUID().toString();
    byte[] content = "hello, world".getBytes(UTF_8);

    InsertRequest insertRequest = new InsertRequest(id, content, 0, 0,
      kvTimeout, core.context(), CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(), Optional.empty(), null);
    core.send(insertRequest);

    InsertResponse insertResponse = insertRequest.response().get();
    assertTrue(insertResponse.status().success());

    GetRequest getRequest = new GetRequest(id, kvTimeout,
      core.context(), CollectionIdentifier.fromDefault(config().bucketname()), env.retryStrategy(), null);
    core.send(getRequest);

    GetResponse getResponse = getRequest.response().get();
    assertTrue(getResponse.status().success());
    assertArrayEquals(content, getResponse.content());
    assertTrue(getResponse.cas() != 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  void failsIfMoreThanOneTrustPresent() {
    assertThrows(InvalidArgumentException.class, () -> secureEnvironment(SecurityConfig
      .enableTls(true)
      .trustManagerFactory(mock(TrustManagerFactory.class))
      .trustCertificates(mock(List.class)), null)
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  void failsIfWrongCertPresent() throws Exception {
    SimpleEventBus eventBus = new SimpleEventBus(true);
    try (
      CoreEnvironment env = secureEnvironment(SecurityConfig.enableTls(true).trustCertificates(mock(List.class)),
          eventBus);
      Core core = Core.create(env, authenticator(), secureSeeds())) {

        core.openBucket(config().bucketname());

        waitUntilCondition(() -> {
          boolean hasEndpointConnectFailedEvent = false;
          boolean hasSecureConnectionFailedEvent = false;
          for (Event event : eventBus.publishedEvents()) {
            if (event instanceof EndpointConnectionFailedEvent) {
              hasEndpointConnectFailedEvent = true;
            }
            if (event instanceof SecureConnectionFailedEvent) {
              hasSecureConnectionFailedEvent = true;
            }
          }

        return hasEndpointConnectFailedEvent && hasSecureConnectionFailedEvent;
      });
    }
  }

  private Core createCore(CoreEnvironment env, Authenticator auth, Set<SeedNode> seedNodes) throws ExecutionException, InterruptedException, TimeoutException {
    Core core = Core.create(env, auth, seedNodes);
    core.openBucket(config().bucketname());
    core.waitUntilReady(serviceTypes, Duration.ofSeconds(10), ClusterState.ONLINE, config().bucketname())
      .get(10, TimeUnit.SECONDS);
    return core;
  }
}
