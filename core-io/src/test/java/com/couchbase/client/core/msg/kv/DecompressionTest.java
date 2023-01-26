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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.kv.ProtocolVerifier.decodeHexDump;
import static com.couchbase.client.test.Util.readResource;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Makes sure that responses which have content and the proper flags set also decode the content
 * properly.
 */
class DecompressionTest {

  @Test
  void decompressesGet() {
    ByteBuf response = decodeHexDump(readResource(
      "compressed_get_response.txt",
      DecompressionTest.class
    ));

    GetRequest request = new GetRequest("mydoc", Duration.ofSeconds(1),
      mock(CoreContext.class), CollectionIdentifier.fromDefault("bucket"), BestEffortRetryStrategy.INSTANCE, null);
    GetResponse decoded = request.decode(response, null);

    assertThat(readResource("dummy.json", DecompressionTest.class)).isEqualToIgnoringNewLines(new String(decoded.content(), UTF_8));
  }

  @Test
  void decompressesReplicaGet() {
    ByteBuf response = decodeHexDump(readResource(
      "compressed_replica_get_response.txt",
      DecompressionTest.class
    ));

    ReplicaGetRequest request = new ReplicaGetRequest("mydoc", Duration.ofSeconds(1),
      mock(CoreContext.class), CollectionIdentifier.fromDefault("bucket"), BestEffortRetryStrategy.INSTANCE, (short) 1, null);
    GetResponse decoded = request.decode(response, null);

    assertThat(readResource("dummy.json", DecompressionTest.class))
      .isEqualToIgnoringNewLines(new String(decoded.content(), UTF_8));
  }

  @Test
  void decompressesGetAndLock() {
    ByteBuf response = decodeHexDump(readResource(
      "compressed_get_and_lock_response.txt",
      DecompressionTest.class
    ));

    GetAndLockRequest request = new GetAndLockRequest("mydoc",
      Duration.ofSeconds(1), mock(CoreContext.class), CollectionIdentifier.fromDefault("bucket"),
      BestEffortRetryStrategy.INSTANCE, Duration.ofSeconds(1), null);
    GetAndLockResponse decoded = request.decode(response, null);

    assertThat(readResource("dummy.json", DecompressionTest.class))
      .isEqualToIgnoringNewLines(new String(decoded.content(), UTF_8));
  }

  @Test
  void decompressesGetAndTouch() {
    ByteBuf response = decodeHexDump(readResource(
      "compressed_get_and_touch_response.txt",
      DecompressionTest.class
    ));

    long encodedExpiry = 1;
    GetAndTouchRequest request = new GetAndTouchRequest("mydoc",
      Duration.ofSeconds(1), mock(CoreContext.class), CollectionIdentifier.fromDefault("bucket"),
      BestEffortRetryStrategy.INSTANCE, encodedExpiry, null);
    GetAndTouchResponse decoded = request.decode(response, null);

    assertThat(readResource("dummy.json", DecompressionTest.class))
      .isEqualToIgnoringNewLines(new String(decoded.content(), UTF_8));
  }

}
