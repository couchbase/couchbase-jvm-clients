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

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.IoEnvironment;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.InsertResponse;
import com.couchbase.client.core.util.CoreIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Services;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.deps.io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import javax.net.ssl.TrustManagerFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

/**
 * Verifies that the core is able to connect and perform operations over encrypted
 * transports.
 *
 * <p>Note that since the mock does not support encrypted connections, they are ignored
 * on it.</p>
 */
class TransportEncryptionIntegrationTest extends CoreIntegrationTest {

  /**
   * Helper method to configure the secure environment based on the integration seed nodes
   * from the target cluster and the security config from each test.
   *
   * @param config the security config to use.
   * @return a core environment, set up for encrypted networking.
   */
  private CoreEnvironment secureEnvironment(final SecurityConfig.Builder config) {
    Set<SeedNode> seeds = config().nodes().stream().map(cfg -> SeedNode.create(
      cfg.hostname(),
      Optional.of(cfg.ports().get(Services.KV_TLS)),
      Optional.of(cfg.ports().get(Services.MANAGER_TLS))
    )).collect(Collectors.toSet());

    return environment()
      .securityConfig(config)
      .seedNodes(seeds)
      .build();
  }

  @Test
  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED })
  void performsKeyValueIgnoringServerCert() throws Exception {
    CoreEnvironment env = secureEnvironment(SecurityConfig
      .tlsEnabled(true)
      .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE));
    Core core = Core.create(env);
    core.openBucket(config().bucketname()).block();

    try {
      String id = UUID.randomUUID().toString();
      byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

      InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
        Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
      core.send(insertRequest);

      InsertResponse insertResponse = insertRequest.response().get();
      assertTrue(insertResponse.status().success());

      GetRequest getRequest = new GetRequest(id, null, Duration.ofSeconds(1),
        core.context(), config().bucketname(), env.retryStrategy());
      core.send(getRequest);

      GetResponse getResponse = getRequest.response().get();
      assertTrue(getResponse.status().success());
      assertArrayEquals(content, getResponse.content());
      assertTrue(getResponse.cas() != 0);
    } finally {
      core.shutdown().block();
      env.shutdown();
    }
  }

  @Test
  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED })
  void performsKeyValueWithServerCert() throws Exception {
    CoreEnvironment env = secureEnvironment(SecurityConfig
      .tlsEnabled(true)
      .trustCertificates(config().clusterCert().get()));
    Core core = Core.create(env);
    core.openBucket(config().bucketname()).block();

    try {
      String id = UUID.randomUUID().toString();
      byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

      InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
        Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy(), Optional.empty());
      core.send(insertRequest);

      InsertResponse insertResponse = insertRequest.response().get();
      assertTrue(insertResponse.status().success());

      GetRequest getRequest = new GetRequest(id, null, Duration.ofSeconds(1),
        core.context(), config().bucketname(), env.retryStrategy());
      core.send(getRequest);

      GetResponse getResponse = getRequest.response().get();
      assertTrue(getResponse.status().success());
      assertArrayEquals(content, getResponse.content());
      assertTrue(getResponse.cas() != 0);
    } finally {
      core.shutdown().block();
      env.shutdown();
    }
  }

  @Test
  void failsIfNoTrustPresent() {
    assertThrows(IllegalArgumentException.class, () -> secureEnvironment(SecurityConfig.tlsEnabled(true)));
  }

  @Test
  void failsIfMoreThanOneTrustPresent() {
    assertThrows(IllegalArgumentException.class, () -> secureEnvironment(SecurityConfig
      .tlsEnabled(true)
      .trustManagerFactory(mock(TrustManagerFactory.class))
      .trustCertificates(mock(X509Certificate.class)))
    );
  }

  @Test
  @IgnoreWhen(clusterTypes = { ClusterType.MOCKED })
  void failsIfWrongCertPresent() {
    CoreEnvironment env = secureEnvironment(SecurityConfig
      .tlsEnabled(true)
      .trustCertificates(mock(X509Certificate.class)));
    Core core = Core.create(env);

    // Todo: this must not throw, but the op underneath timeout! .. also assert based
    // on status of the bucket...
    assertThrows(Exception.class, () -> core.openBucket(config().bucketname()).block());

    try {
      /*String id = UUID.randomUUID().toString();
      byte[] content = "hello, world".getBytes(CharsetUtil.UTF_8);

      InsertRequest insertRequest = new InsertRequest(id, null, content, 0, 0,
        Duration.ofSeconds(1), core.context(), config().bucketname(), env.retryStrategy());
      core.send(insertRequest);

      InsertResponse insertResponse = insertRequest.response().get();
      assertTrue(insertResponse.status().success());*/
    } finally {
      core.shutdown().block();
      env.shutdown();
    }
  }

}
