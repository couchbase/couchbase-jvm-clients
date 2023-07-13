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
import com.couchbase.client.core.deps.io.netty.util.ReferenceCountUtil;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol.Datatype;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality provided by {@link MemcacheProtocol}.
 */
class MemcacheProtocolTest {

  private static final ByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;
  private CoreContext context;
  private EventBus eventBus;

  @BeforeEach
  void before() {
    eventBus = mock(EventBus.class);
    CoreEnvironment env = mock(CoreEnvironment.class);
    context = new CoreContext(mock(Core.class), 1, env, mock(Authenticator.class));
    when(env.eventBus()).thenReturn(eventBus);
  }

  @Test
  void canParseMagic() {
    assertCanParseEnum(
      MemcacheProtocol.Magic.class,
      MemcacheProtocol.Magic::magic,
      MemcacheProtocol.Magic::of,
      (byte) -1
    );
  }

  @Test
  void canParseOpcode() {
    assertCanParseEnum(
      MemcacheProtocol.Opcode.class,
      MemcacheProtocol.Opcode::opcode,
      MemcacheProtocol.Opcode::of,
      (byte) -1
    );
  }

  @Test
  void canParseServerPushOpcode() {
    assertCanParseEnum(
      MemcacheProtocol.ServerPushOpcode.class,
      MemcacheProtocol.ServerPushOpcode::opcode,
      MemcacheProtocol.ServerPushOpcode::of,
      (byte) -1
    );
  }

  @Test
  void canParseStatus() {
    assertCanParseEnum(
      MemcacheProtocol.Status.class,
      MemcacheProtocol.Status::status,
      MemcacheProtocol.Status::of,
      (short) -1
    );
  }

  private static <ENUM extends Enum<ENUM>, CODE extends Number> void assertCanParseEnum(
    Class<ENUM> enumClass,
    Function<ENUM, CODE> enumToCode,
    Function<CODE, ENUM> codeToEnum,
    CODE unrecognizedCode // for which codeToEnum returns null
  ) {
    for (ENUM e : enumClass.getEnumConstants()) {
      assertEquals(e, codeToEnum.apply(enumToCode.apply(e)));
    }
    assertNull(codeToEnum.apply(unrecognizedCode));
  }

  @Test
  void flexibleSyncReplicationUserTimeout() {
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(
        ALLOC.buffer(),
        DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
        Duration.ofSeconds(3),
        context
    );
    try {
      assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
      assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
      assertEquals(0x02, result.getByte(1)); // majority and persist on active has id 2
      assertEquals(2700, result.getShort(2)); // 2700 -> 90% of the 3000ms user timeout

      verify(eventBus, never()).publish(any(DurabilityTimeoutCoercedEvent.class));
    } finally {
      ReferenceCountUtil.release(result);
    }
  }

  @Test
  void flexibleSyncReplicationTimeoutFloor() {
    Duration tooLowTimeout = Duration.ofSeconds(1);
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC.buffer(), DurabilityLevel.MAJORITY, tooLowTimeout, context);
    try {
      assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
      assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
      assertEquals(0x01, result.getByte(1)); // majority has id of 1
      assertEquals(1500, result.getShort(2)); // we cannot go below 1500 per server spec so no 90%.

      verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
    } finally {
      ReferenceCountUtil.release(result);
    }
  }

  @Test
  void flexibleSyncReplicationTimeoutRightAtFloor() {
    // 1667 is valid, and should not log
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC.buffer(), DurabilityLevel.MAJORITY, Duration.ofMillis(1667), context);
    try {
      assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
      assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
      assertEquals(0x01, result.getByte(1)); // majority has id of 1
      assertEquals(1500, result.getShort(2)); // expect what you asked for (well, 90% of it)
      verify(eventBus, never()).publish(any(DurabilityTimeoutCoercedEvent.class));
    } finally {
      ReferenceCountUtil.release(result);
    }

    // 1666 actually will log...
    result = MemcacheProtocol.flexibleSyncReplication(ALLOC.buffer(), DurabilityLevel.MAJORITY, Duration.ofMillis(1666), context);
    try {
      assertEquals(4, result.readableBytes()); // 4 bytes total for these flexible extras
      assertEquals(0x13, result.getByte(0)); // sync replication id 1 and length 3
      assertEquals(0x01, result.getByte(1)); // majority has id of 1
      assertEquals(1500, result.getShort(2)); // expect what you asked for
      verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
    } finally {
      ReferenceCountUtil.release(result);
    }
  }

  @Test
  void flexibleSyncReplicationTimeoutMaxValues() {
    Duration duration = Duration.ofMillis(65535);
    ByteBuf result = MemcacheProtocol.flexibleSyncReplication(ALLOC.buffer(), DurabilityLevel.MAJORITY, duration, context);
    try {
      assertEquals(65534, result.getUnsignedShort(2));
      verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
      reset(eventBus);
    } finally {
      ReferenceCountUtil.release(result);
    }

    duration = Duration.ofMillis(Long.MAX_VALUE);
    result = MemcacheProtocol.flexibleSyncReplication(ALLOC.buffer(), DurabilityLevel.MAJORITY, duration, context);
    try {
      assertEquals(65534, result.getUnsignedShort(2));
      verify(eventBus, times(1)).publish(any(DurabilityTimeoutCoercedEvent.class));
    } finally {
      ReferenceCountUtil.release(result);
    }
  }

  /**
   * A reminder to update {@link MemcacheProtocol.Status#of} when adding a new status code.
   */
  @Test
  void canFindStatusByCode() {
    for (MemcacheProtocol.Status status : MemcacheProtocol.Status.values()) {
      assertEquals(
        status,
        MemcacheProtocol.Status.of(status.status()),
        "Unexpected result for Status.of(0x" + Integer.toHexString(status.status()) + ")");
    }
  }

  @Test
  void canParseDatatypes() {
    Set<Datatype> allValues = EnumSet.allOf(Datatype.class);
    assertEquals(allValues, Datatype.decode(Datatype.encode(allValues)));
    assertEquals(allValues, Datatype.decode(-1)); // assert ignores unrecognized bits
    assertEquals(emptySet(), Datatype.decode(0));

    for (Datatype d : allValues) {
      int encoded = Datatype.encode(setOf(d));
      assertEquals(d.datatype(), encoded);
      assertEquals(setOf(d), Datatype.decode(encoded));

      for (Datatype v : allValues) {
        assertEquals(d == v, Datatype.contains(encoded, v));
      }
    }

    for (int i = 0; i < 0xff; i++) {
      Set<Datatype> set = Datatype.decode(i);
      int encoded = Datatype.encode(set);
      assertEquals((encoded & i), encoded);
    }
  }
}
