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

package com.couchbase.client.core.env;

import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.service.ServiceType;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuilderPropertySetterTest {
  private static final BuilderPropertySetter setter = new BuilderPropertySetter();

  private static CoreEnvironment.Builder<?> newEnvironmentBuilder() {
    return CoreEnvironment.builder();
  }

  @Test
  void setBoolean() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();
    setter.set(builder, "io.queryCircuitBreaker.enabled", "true");
    setter.set(builder, "io.analyticsCircuitBreaker.enabled", "1");
    setter.set(builder, "io.viewCircuitBreaker.enabled", "false");
    setter.set(builder, "io.kvCircuitBreaker.enabled", "0");

    IoConfig io = builder.ioConfig().build();
    assertTrue(io.queryCircuitBreakerConfig().enabled());
    assertTrue(io.analyticsCircuitBreakerConfig().enabled());
    assertFalse(io.viewCircuitBreakerConfig().enabled());
    assertFalse(io.kvCircuitBreakerConfig().enabled());

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class,
      () -> setter.set(builder, "io.kvCircuitBreaker.enabled", "TRUE"));

    assertEquals("Expected a boolean (\"true\", \"false\", \"1\", or \"0\") but got \"TRUE\".",
      e.getCause().getMessage());
  }

  @Test
  void setInt() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();
    setter.set(builder, "io.maxHttpConnections", "76");

    IoConfig io = builder.ioConfig().build();
    assertEquals(76, io.maxHttpConnections());

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class,
      () -> setter.set(builder, "io.maxHttpConnections", "garbage"));

    assertEquals("Expected an int but got \"garbage\".",
      e.getCause().getMessage());
  }

  @Test
  void setDuration() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();

    setter.set(builder, "io.idleHttpConnectionTimeout", "0");
    setter.set(builder, "io.tcpKeepAliveTime", "2s");

    IoConfig io = builder.ioConfig().build();
    assertEquals(Duration.ZERO, io.idleHttpConnectionTimeout());
    assertEquals(Duration.ofSeconds(2), io.tcpKeepAliveTime());
  }

  @Test
  void rejectNegativeDuration() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();

    assertThrows(InvalidArgumentException.class,
      () -> setter.set(builder, "service.queryService.idleTime", "-3s"));
  }

  @Test
  void setEnumArray() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();

    setter.set(builder, "io.captureTraffic", " KV , ANALYTICS ");

    IoConfig io = builder.ioConfig().build();
    Set<ServiceType> expected = EnumSet.of(ServiceType.KV, ServiceType.ANALYTICS);
    assertEquals(expected, io.servicesToCapture());

    setter.set(builder, "io.captureTraffic", "");
    assertEquals(EnumSet.allOf(ServiceType.class), builder.ioConfig().build().servicesToCapture());


    InvalidArgumentException e = assertThrows(InvalidArgumentException.class,
      () -> setter.set(builder, "io.captureTraffic", "garbage"));

    assertEquals("Expected one of " + EnumSet.allOf(ServiceType.class) + " but got \"garbage\"",
      e.getCause().getMessage());
  }

  @Test
  void setDouble() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();

    setter.set(builder, "compression.minRatio", "3.14159");

    CompressionConfig compression = builder.compressionConfig().build();
    assertEquals(3.14159, compression.minRatio());

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class,
      () -> setter.set(builder, "compression.minRatio", "garbage"));

    assertEquals("Expected a double but got \"garbage\".", e.getCause().getMessage());
  }

  @Test
  void worksWithIoEnvironmentAndPropertyLoaderFromMap() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder()
        .load(PropertyLoader.fromMap(
            mapOf("ioEnvironment.enableNativeIo", "false")));

    IoEnvironment ioEnv = builder.ioEnvironmentConfig().build();
    assertFalse(ioEnv.nativeIoEnabled());

    InvalidPropertyException e = assertThrows(InvalidPropertyException.class, () ->
        builder.load(PropertyLoader.fromMap(
            mapOf("io.maxHttpConnections", "garbage"))));

    assertEquals("io.maxHttpConnections", e.propertyName());
    assertEquals("garbage", e.propertyValue());
  }

  @Test
  void worksWithProfile() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();
    assertEquals(builder.build().timeoutConfig().kvTimeout(), Duration.ofMillis(2500));
    builder.applyProfile("wan-development");
    assertEquals(builder.build().timeoutConfig().kvTimeout(), Duration.ofSeconds(5));
    InvalidArgumentException e1 = assertThrows(InvalidArgumentException.class,
        () -> builder.applyProfile("default"));
    assertEquals("Unknown profile: 'default', valid profiles are: [wan-development]", e1.getMessage());
    InvalidArgumentException e2 = assertThrows(InvalidArgumentException.class,
        () -> builder.applyProfile(null));
    assertEquals("ProfileName cannot be null or empty", e2.getMessage());
  }

  @Test
  void handyMethodsToModifyConfigs() {
    CoreEnvironment.Builder<?> builder = newEnvironmentBuilder();
    builder.applyProfile("wan-development");
    builder.ioConfig(config -> config.numKvConnections(2)).timeoutConfig(config -> config.queryTimeout(Duration.ofSeconds(100)));
    CoreEnvironment env = builder.build();
    assertEquals(env.timeoutConfig().kvTimeout(), Duration.ofSeconds(5)); // from development profile
    assertEquals(env.timeoutConfig().queryTimeout(), Duration.ofSeconds(100)); // set above
    assertEquals(2, env.ioConfig().numKvConnections()); // set above
    assertEquals(12, env.ioConfig().maxHttpConnections()); // from defaults
  }

  @Test
  void triesAllOverloads() {
    HasOverloads overloads = new HasOverloads();
    setter.set(overloads, "value", "23");
    setter.set(overloads, "value", "2s");
    setter.set(overloads, "value", "{\"luckyNumber\":37}");

    assertEquals(23, overloads.i);
    assertEquals(Duration.ofSeconds(2), overloads.d);
    assertEquals(singletonMap("luckyNumber", 37), overloads.m);

    InvalidArgumentException e = assertThrows(InvalidArgumentException.class,
      () -> setter.set(overloads, "value", "garbage"));

    assertTrue(e.getCause().getMessage().startsWith("Found multiple one-arg setters"));
    final Set<String> suppressedMessages = Arrays.stream(e.getCause().getSuppressed()).map(Throwable::getMessage).collect(toSet());
    final Set<String> expectedSuppressedMessages = new HashSet<>(Arrays.asList(
      "Expected an int but got \"garbage\".",
      "Expected a duration qualified by a time unit (like \"2.5s\" or \"300ms\") but got \"garbage\".",
      "Expected a value Jackson can bind to java.util.Map<java.lang.String, java.lang.Integer> but got \"garbage\"."));
    assertEquals(expectedSuppressedMessages, suppressedMessages);
  }

  private static class HasOverloads {
    int i;
    Duration d;
    Map<String, Integer> m;

    public void value(int i) {
      this.i = i;
    }

    public void value(Duration d) {
      this.d = d;
    }

    public void value(Map<String, Integer> m) {
      this.m = m;
    }
  }

}
