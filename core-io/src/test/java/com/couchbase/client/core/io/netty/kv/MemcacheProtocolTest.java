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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the functionality provided by {@link MemcacheProtocol}.
 */
class MemcacheProtocolTest {

  private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

  @Test
  void flexibleSyncReplicationUserTimeout() {
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(
      ALLOC,
      DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER,
      Duration.ofSeconds(3)
    );

    assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
    assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
    assertEquals(0x02, result.getByte(1)); // majority and persist on master has id 2
    assertEquals(2700, result.getShort(2)); // 2700 -> 90% of the 3000ms user timeout

    result.release();
  }

  @Test
  void flexibleSyncReplicationTimeoutFloor() {
    Duration tooLowTimeout = Duration.ofSeconds(1);
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, tooLowTimeout);

    assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
    assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
    assertEquals(0x01, result.getByte(1)); // majority has id of 1
    assertEquals(1500, result.getShort(2)); // we cannot go below 1500 per server spec so no 90%.

    result.release();
  }

  @Test
  void flexibleSyncReplicationTimeoutMaxValues() {
    Duration duration = Duration.ofMillis(65535);
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, duration);
    assertEquals(65534, result.getUnsignedShort(2));
    result.release();

    duration = Duration.ofMillis(Long.MAX_VALUE);
    result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, duration);
    assertEquals(65534, result.getUnsignedShort(2));
    result.release();
  }


}
