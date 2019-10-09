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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.events.io.DurabilityTimeoutCoercedEvent;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBufAllocator;
import com.couchbase.client.core.deps.io.netty.buffer.UnpooledByteBufAllocator;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Verifies the functionality provided by {@link MemcacheProtocol}.
 */
class MemcacheProtocolTest {

  private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;
  CoreContext context;
  EventBus eventBus;

  @BeforeEach
  void before() {
    eventBus = mock(EventBus.class);
    CoreEnvironment env = mock(CoreEnvironment.class);
    context = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    when(env.eventBus()).thenReturn(eventBus);
  }

  @Test
  void flexibleSyncReplicationUserTimeout() {
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(
      ALLOC,
      DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER,
      Duration.ofSeconds(3),
      context
    );

    assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
    assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
    assertEquals(0x02, result.getByte(1)); // majority and persist on master has id 2
    assertEquals(2700, result.getShort(2)); // 2700 -> 90% of the 3000ms user timeout
    verify(eventBus, never()).publish(any(DurabilityTimeoutCoercedEvent.class));

    result.release();
  }

  @Test
  void flexibleSyncReplicationTimeoutFloor() {
    Duration tooLowTimeout = Duration.ofSeconds(1);
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, tooLowTimeout, context);

    assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
    assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
    assertEquals(0x01, result.getByte(1)); // majority has id of 1
    assertEquals(1500, result.getShort(2)); // we cannot go below 1500 per server spec so no 90%.

    verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
    result.release();
  }

  @Test
  void flexibleSyncReplicationTimeoutRightAtFloor() {
    // 1667 is valid, and should not log
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, Duration.ofMillis(1667), context);
    assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
    assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
    assertEquals(0x01, result.getByte(1)); // majority has id of 1
    assertEquals(1500, result.getShort(2)); // expect what you asked for (well, 90% of it)
    verify(eventBus, never()).publish(any(DurabilityTimeoutCoercedEvent.class));

    // 1666 actually will log...
    result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, Duration.ofMillis(1666), context);
    assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
    assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
    assertEquals(0x01, result.getByte(1)); // majority has id of 1
    assertEquals(1500, result.getShort(2)); // expect what you asked for
    verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
  }

  @Test
  void flexibleSyncReplicationTimeoutMaxValues() {
    Duration duration = Duration.ofMillis(65535);
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, duration, context);
    assertEquals(65534, result.getUnsignedShort(2));
    verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
    reset(eventBus);
    result.release();

    duration = Duration.ofMillis(Long.MAX_VALUE);
    result = MemcacheProtocol.flexibleSyncReplication(ALLOC, DurabilityLevel.MAJORITY, duration, context);
    assertEquals(65534, result.getUnsignedShort(2));
    verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
    result.release();
  }


}
