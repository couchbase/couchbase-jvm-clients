/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import org.junit.jupiter.api.Test;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MutationTokenAggregatorTest {
  @Test
  void canExportAndImport() {
    String bucketName = "myBucket";
    MutationToken t1 = new MutationToken((short) 0, 12345, 1, bucketName);
    MutationToken t2 = new MutationToken((short) 2, -72613, 2, bucketName);
    MutationToken t3 = new MutationToken((short) 2, -72613, 1, bucketName);

    MutationTokenAggregator tokens = new MutationTokenAggregator();
    tokens.add(t1);
    tokens.add(t2);
    tokens.add(t3); // should be ignored, since same partition and lower seqno than t2

    assertEquals(
      mapOf(
        bucketName, mapOf(
          "0", listOf(1L, "12345"),
          "2", listOf(2L, "-72613")
        )
      ),
      tokens.export()
    );

    MutationTokenAggregator roundTrip = MutationTokenAggregator.from(tokens.export());
    assertEquals(roundTrip, tokens);
  }

}
