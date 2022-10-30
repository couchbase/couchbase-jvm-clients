/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.SerializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.test.Util.readResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

class KetamaRingTest {

  private static final JsonMapper mapper = JsonMapper.builder()
      .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
      .enable(DeserializationFeature.USE_LONG_FOR_INTS)
      .build();

  // From SDK RFC 26
  private static final KetamaRing<String> ring = new KetamaRing<>(
      listOf(
          "192.168.1.101:11210",
          "192.168.1.102:11210",
          "192.168.1.103:11210",
          "192.168.1.104:11210"
      ),
      (value, repetition) -> value + "-" + repetition
  );

  /**
   * Verification dataset comes from
   * <a href="https://github.com/couchbaselabs/sdk-rfcs/blob/master/rfc/0026-ketama-hashing.md">
   * Couchbase SDK RFC 26 (Ketama Hashing)
   * </a>
   */
  @Test
  public void matchesVerificationDataset() throws Exception {
    JsonNode expected = mapper.readTree(readResource("ketama-hashes.json", KetamaRingTest.class));
    JsonNode actual = formatForVerification(ring);

    assertEquals(expected.toPrettyString(), actual.toPrettyString());
    assertEquals(expected, actual);
  }

  private static JsonNode formatForVerification(KetamaRing<?> c) {
    List<Map<String, Object>> results = transform(
        c.toMap().entrySet(),
        entry -> mapOf(
            "hash", entry.getKey(),
            "hostname", entry.getValue()
        )
    );
    return mapper.convertValue(results, JsonNode.class);
  }

  @Test
  public void wrapsAround() {
    assertEquals("192.168.1.104:11210", ring.get(4294967295L));
  }

  @Test
  public void lastValueWinsIfCollision() {
    KetamaRing<String> ringWithCollision = new KetamaRing<>(
        listOf(
            "example.com:11210",
            "example137723.com:11210"
        ),
        (value, repetition) -> value + "-" + repetition
    );

    long expectedCollisionPoint = 1723476360L;

    // Expect a single collision (both nodes mapped to the same point on the ring).
    int expectedCollisions = 1;
    int pointsPerValue = 160;
    assertEquals((pointsPerValue * 2) - expectedCollisions, ringWithCollision.toMap().size());

    // The value that appears later in original list should win.
    assertEquals(
        "example137723.com:11210",
        ringWithCollision.toMap().get(expectedCollisionPoint)
    );
  }
}
