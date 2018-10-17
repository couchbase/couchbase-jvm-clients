/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.io.netty.kv.sasl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link ScramSaslClientFactory}.
 */
class ScramSaslClientFactoryTest {

  @Test
  void containSupportedMechanisms() {
    String[] supported = new ScramSaslClientFactory().getMechanismNames(null);
    assertArrayEquals(new String[] {
      "SCRAM-SHA512",
      "SCRAM-SHA256",
      "SCRAM-SHA1"
    }, supported);
  }

  /**
   * Note that this test also checks that the checking is case-insensitive.
   */
  @Test
  void extractModeFromString() {
    assertEquals(
      ScramSaslClientFactory.Mode.SCRAM_SHA1,
      ScramSaslClientFactory.Mode.from("SCRAM-SHA1").get()
    );
    assertEquals(
      ScramSaslClientFactory.Mode.SCRAM_SHA256,
      ScramSaslClientFactory.Mode.from("SCRAM-sHa256").get()
    );
    assertEquals(
      ScramSaslClientFactory.Mode.SCRAM_SHA512,
      ScramSaslClientFactory.Mode.from("scram-sha512").get()
    );
    assertFalse(ScramSaslClientFactory.Mode.from("CRAM-MD5").isPresent());
    assertFalse(ScramSaslClientFactory.Mode.from("PLAIN").isPresent());
    assertFalse(ScramSaslClientFactory.Mode.from("foobar").isPresent());
  }

  @Test
  void failWithUnsupportedMechanism() throws Exception {
    ScramSaslClientFactory factory = new ScramSaslClientFactory();
    CallbackHandler cbh = mock(CallbackHandler.class);

    SaslClient client = factory.createSaslClient(new String[] { "CRAM-MD5" }, null, null, null, null, cbh);
    assertNull(client);
  }

  @Test
  void succeedWithSupportedMechanism() throws Exception {
    ScramSaslClientFactory factory = new ScramSaslClientFactory();
    CallbackHandler cbh = mock(CallbackHandler.class);

    SaslClient client = factory.createSaslClient(new String[] { "SCRAM-SHA256", "SCRAM-SHA512" }, null, null, null, null, cbh);
    assertNotNull(client);
    assertEquals("SCRAM-SHA512", client.getMechanismName());
  }

  @ParameterizedTest
  @MethodSource
  void selectStrongestOption(final InputHolder inputHolder) {
    assertEquals(inputHolder.expected, ScramSaslClientFactory.Mode.strongestOf(inputHolder.mechs));
  }

  static Stream<InputHolder> selectStrongestOption() {
    return Stream.of(
      new InputHolder(
        new String[] {"SCRAM-SHA512"},
        Optional.of(ScramSaslClientFactory.Mode.SCRAM_SHA512)
      ),
      new InputHolder(
        new String[] {"SCRAM-SHA512", "SCRAM-SHA256", "SCRAM-SHA1"},
        Optional.of(ScramSaslClientFactory.Mode.SCRAM_SHA512)
      ),
      new InputHolder(
        new String[] {"SCRAM-SHA1", "CRAM-MD5", "PLAIN"},
        Optional.of(ScramSaslClientFactory.Mode.SCRAM_SHA1)
      ),
      new InputHolder(new String[] {"PLAIN"}, Optional.empty())
    );
  }

  /**
   * Simple holder which allows us to give a name to the parameterized
   * function.
   */
  private static class InputHolder {
    final String[] mechs;
    final Optional<ScramSaslClientFactory.Mode> expected;

    public InputHolder(String[] mechs, Optional<ScramSaslClientFactory.Mode> expected) {
      this.mechs = mechs;
      this.expected = expected;
    }

    @Override
    public String toString() {
      return Arrays.toString(mechs) + ": " + expected;
    }
  }

}