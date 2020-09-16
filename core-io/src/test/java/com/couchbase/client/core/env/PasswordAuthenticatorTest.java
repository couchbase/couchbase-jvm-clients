/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link PasswordAuthenticator}.
 */
class PasswordAuthenticatorTest {

  /**
   * Holds a basic environment without tls enabled.
   */
  static CoreEnvironment ENV;

  @BeforeAll
  static void beforeAll() {
    ENV = CoreEnvironment.create();
  }

  @AfterAll
  static void afterAll() {
    ENV.shutdown();
  }

  @Test
  void shouldNotNegotiatePlainWithNonTlsByDefault() {
    PasswordAuthenticator authenticator = PasswordAuthenticator.create("user", "pass");

    EndpointContext ctx = mock(EndpointContext.class);
    when(ctx.environment()).thenReturn(ENV);

    EmbeddedChannel channel = new EmbeddedChannel();
    authenticator.authKeyValueConnection(ctx, channel.pipeline());

    SaslAuthenticationHandler handler = channel.pipeline().get(SaslAuthenticationHandler.class);
    assertFalse(handler.allowedMechanisms().contains(SaslMechanism.PLAIN));
  }

  @Test
  void allowsToEnablePlainInAddition() {
    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder()
      .username("user")
      .password("pass")
      .enablePlainSaslMechanism()
      .build();

    EndpointContext ctx = mock(EndpointContext.class);
    when(ctx.environment()).thenReturn(ENV);

    EmbeddedChannel channel = new EmbeddedChannel();
    authenticator.authKeyValueConnection(ctx, channel.pipeline());

    SaslAuthenticationHandler handler = channel.pipeline().get(SaslAuthenticationHandler.class);
    assertEquals(EnumSet.allOf(SaslMechanism.class), handler.allowedMechanisms());
  }

  @Test
  void ldapCompatibleOnlyEnablesPlain() {
    PasswordAuthenticator authenticator = PasswordAuthenticator.ldapCompatible("user", "pass");

    EndpointContext ctx = mock(EndpointContext.class);
    when(ctx.environment()).thenReturn(ENV);

    EmbeddedChannel channel = new EmbeddedChannel();
    authenticator.authKeyValueConnection(ctx, channel.pipeline());

    SaslAuthenticationHandler handler = channel.pipeline().get(SaslAuthenticationHandler.class);
    assertEquals(EnumSet.of(SaslMechanism.PLAIN), handler.allowedMechanisms());
  }

  /**
   * Regression test for JVMCBC-890.
   */
  @Test
  void shouldOnlyNegotiatePlainWhenTlsEnabled() {
    PasswordAuthenticator authenticator = PasswordAuthenticator.create("user", "pass");

    CoreEnvironment tlsEnvironment = CoreEnvironment.builder().securityConfig(SecurityConfig
      .enableTls(true)
      .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
    ).build();

    try {
      EndpointContext ctx = mock(EndpointContext.class);
      when(ctx.environment()).thenReturn(tlsEnvironment);

      EmbeddedChannel channel = new EmbeddedChannel();
      authenticator.authKeyValueConnection(ctx, channel.pipeline());

      SaslAuthenticationHandler handler = channel.pipeline().get(SaslAuthenticationHandler.class);
      assertEquals(EnumSet.of(SaslMechanism.PLAIN), handler.allowedMechanisms());
    } finally {
      tlsEnvironment.shutdown();
    }
  }

}