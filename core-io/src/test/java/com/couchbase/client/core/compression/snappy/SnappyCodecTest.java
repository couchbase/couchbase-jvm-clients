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

package com.couchbase.client.core.compression.snappy;

import com.couchbase.client.core.deps.org.iq80.snappy.Snappy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class SnappyCodecTest {

  // Triggers infinite loop in incrementalCopyFastPath with official orq.iq80.snappy version 0.4.
  // Our patched version should throw an exception instead.
  private static final byte[] INVALID_CAUSES_HANG = new byte[]{-103, 31, 1, 0, 78, -86, -32, -94, 97, -66};

  // Invalid input, causes JVM crash with official orq.iq80.snappy version 0.4.
  // Our patched version should throw an exception instead.
  private static final byte[] INVALID_CAUSES_JVM_CRASH = new byte[]{125, 23, 93, -97, 32, -12};

  private static SnappyCodec slowCodec() {
    return new SlowSnappyCodec();
  }

  private static SnappyCodec fastCodec() {
    return new FastSnappyCodec();
  }

  private static void assumeCanUseFastCodec() {
    boolean canUseFastCodec = SnappyCodec.instance() instanceof FastSnappyCodec;
    assumeTrue(
      canUseFastCodec,
      "This test can only run on little-endian hardware in a JVM where `sun.misc.Unsafe` is accessible."
    );
  }

  private static void assumeAssertionsDisabled() {
    assumeFalse(
      Snappy.class.desiredAssertionStatus(), // close enough
      "This tests requires assertions be *disabled*." +
        " Version 0.4 of the Snappy library used assertions to enforce preconditions." +
        " This used to cause a JVM crash when assertions were disabled." +
        " We want to make sure the crash is no longer possible, even when assertions are disabled."
    );
  }

  @Test
  void fastAndSlowAreCompatible() {
    assumeCanUseFastCodec();

    SnappyCodec slowCodec = slowCodec();
    SnappyCodec fastCodec = fastCodec();

    byte[] uncompressed = "Lorem ipsum dolor sit amet. Again: Lorem ipsum dolor sit amet.".getBytes(UTF_8);

    byte[] slowCompressed = slowCodec.compress(uncompressed);
    byte[] fastCompressed = fastCodec.compress(uncompressed);

    // Generally speaking, Snappy compressors are not required to generate the same output,
    // so don't need to assertArrayEquals(slowCompressed, fastCompressed).
    // However, this particular input is highly compressible, so:
    assertThat(slowCompressed.length).isLessThan(uncompressed.length);
    assertThat(fastCompressed.length).isLessThan(uncompressed.length);

    // The codecs should be able to decompress each other's output as well as their own.
    assertArrayEquals(uncompressed, slowCodec.decompress(slowCompressed));
    assertArrayEquals(uncompressed, slowCodec.decompress(fastCompressed));
    assertArrayEquals(uncompressed, fastCodec.decompress(slowCompressed));
    assertArrayEquals(uncompressed, fastCodec.decompress(fastCompressed));
  }

  @Test
  void slowCodecInvalidInputDoesNotCauseHang() {
    assertTimeoutPreemptively(
      Duration.ofSeconds(30),
      () -> assertThrows(Throwable.class, () -> slowCodec().decompress(INVALID_CAUSES_HANG))
    );
  }

  @Test
  void fastCodecInvalidInputDoesNotCauseHang() {
    assumeCanUseFastCodec();
    assertTimeoutPreemptively(
      Duration.ofSeconds(30),
      () -> assertThrows(Throwable.class, () -> fastCodec().decompress(INVALID_CAUSES_HANG))
    );
  }

  @Test
  void slowCodecInvalidInputDoesNotCrashJvm() {
    assumeAssertionsDisabled();
    assertThrows(Throwable.class, () -> slowCodec().decompress(INVALID_CAUSES_JVM_CRASH));
  }

  @Test
  void fastCodecInvalidInputDoesNotCrashJvm() {
    assumeAssertionsDisabled();
    assumeCanUseFastCodec();
    assertThrows(Throwable.class, () -> fastCodec().decompress(INVALID_CAUSES_JVM_CRASH));
  }
}
