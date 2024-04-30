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

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.deps.io.netty.channel.embedded.EmbeddedChannel;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.EnumSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
  void passwordNotIncludedInToString() {
    String pw = "swordfish";
    assertFalse(new UsernameAndPassword("user", pw).toString().contains(pw));
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
  void canUseDeprecatedMethodsToSpecifyStaticCredentials() {
    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder()
      .username("user")
      .password("pass")
      .build();

    // should return the same cached String instance
    assertSame(
      authenticator.getAuthHeaderValue(),
      authenticator.getAuthHeaderValue()
    );
  }

  @Test
  void canUseDeprecatedMethodsToSpecifyDynamicPassword() {
    CountingSupplier<String> passwordSupplier = new CountingSupplier<>(() -> "pass");

    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder()
      .username("user")
      .password(passwordSupplier)
      .build();

    assertNotSame(
      authenticator.getAuthHeaderValue(),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(
      expectedAuthHeaderValue("user", "pass"),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(3, passwordSupplier.count());
  }

  @Test
  void canUseDeprecatedMethodsToSpecifyDynamicUsernameAndPassword() {
    CountingSupplier<String> usernameSupplier = new CountingSupplier<>(() -> "user");
    CountingSupplier<String> passwordSupplier = new CountingSupplier<>(() -> "pass");

    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder()
      .username(usernameSupplier)
      .password(passwordSupplier)
      .build();

    assertNotSame(
      authenticator.getAuthHeaderValue(),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(
      expectedAuthHeaderValue("user", "pass"),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(3, usernameSupplier.count());
    assertEquals(3, passwordSupplier.count());
  }

  @Test
  void cachesEncodedFormOfStaticCredentials() {
    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder("user", "pass")
      .build();

    // should return the same cached String instance
    assertSame(
      authenticator.getAuthHeaderValue(),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(
      expectedAuthHeaderValue("user", "pass"),
      authenticator.getAuthHeaderValue()
    );
  }

  @Test
  void supplierInvokedEveryTime() {
    CountingSupplier<UsernameAndPassword> supplier = new CountingSupplier<>(
      () -> new UsernameAndPassword("user", "pass")
    );

    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder(supplier)
      .build();

    // should not be cached, because dynamic!
    assertNotSame(
      authenticator.getAuthHeaderValue(),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(
      expectedAuthHeaderValue("user", "pass"),
      authenticator.getAuthHeaderValue()
    );

    assertEquals(3, supplier.count());
  }

  @Test
  void depprecatedMethodsAreNotCompatibleWithModernConstructor() {
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder("user", "pass").username("foo"));
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder("user", "pass").username(() -> "foo"));
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder("user", "pass").password("foo"));
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder("user", "pass").password(() -> "foo"));

    Supplier<UsernameAndPassword> supplier = () -> new UsernameAndPassword("user", "pass");
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder(supplier).username("foo"));
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder(supplier).username(() -> "foo"));
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder(supplier).password("foo"));
    assertThrows(IllegalStateException.class, () -> PasswordAuthenticator.builder(supplier).password(() -> "foo"));
  }

  @Test
  void allowsToEnablePlainInAddition() {
    PasswordAuthenticator authenticator = PasswordAuthenticator
      .builder("user", "pass")
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

  @Test
  void onlyEnablePlainFailsIfPlainNotAvailable() {
    Exception e = assertThrows(Exception.class, () ->
      PasswordAuthenticator.builder("user", "pass")
        .setPlatformHasSaslPlain(() -> false)
        .onlyEnablePlainSaslMechanism()
    );
    assertTrue(e.getMessage().contains("PLAIN"));
  }

  /**
   * Regression test for JVMCBC-890.
   */
  @Test
  void shouldOnlyNegotiatePlainWhenTlsEnabled() {
    PasswordAuthenticator authenticator = PasswordAuthenticator.create("user", "pass");

    CoreEnvironment tlsEnvironment = CoreEnvironment.builder()
      .eventBus(new SimpleEventBus(true)) // event bus swallows warnings from insecure trust manager
      .securityConfig(security -> security
        .enableTls(true)
        .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE)
      )
      .build();

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

  private static String expectedAuthHeaderValue(String username, String password) {
    byte[] encodeMe = (username + ":" + password).getBytes(UTF_8);
    return "Basic " + Base64.getEncoder().encodeToString(encodeMe);
  }

  private static class CountingSupplier<T> implements Supplier<T> {
    private final LongAdder count = new LongAdder();
    private final Supplier<T> wrapped;

    public CountingSupplier(Supplier<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public T get() {
      count.increment();
      return wrapped.get();
    }

    public long count() {
      return count.sum();
    }
  }
}
