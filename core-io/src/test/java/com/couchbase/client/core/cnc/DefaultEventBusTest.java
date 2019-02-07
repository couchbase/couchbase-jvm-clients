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

package com.couchbase.client.core.cnc;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.couchbase.client.util.Assertions.assertThreadNotRunning;
import static com.couchbase.client.util.Assertions.assertThreadRunning;
import static com.couchbase.client.util.Utils.threadRunning;
import static com.couchbase.client.util.Utils.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Verifies the functionality of the {@link DefaultEventBus}.
 */
class DefaultEventBusTest {

  @Test
  void startAndStopEventBus() {
    final String threadName = UUID.randomUUID().toString();

    DefaultEventBus eventBus = DefaultEventBus
      .builder()
      .threadName(threadName)
      .build();
    assertFalse(eventBus.isRunning());
    assertThreadNotRunning(threadName);

    eventBus.start().block();
    assertTrue(eventBus.isRunning());
    assertThreadRunning(threadName);

    eventBus.stop().block();
    waitUntilCondition(() -> !threadRunning(threadName));
    assertFalse(eventBus.isRunning());
    assertThreadNotRunning(threadName);
  }

  @Test
  void subscribeAndUnsubscribeFromEventBus() {
    DefaultEventBus eventBus = DefaultEventBus.create();

    assertFalse(eventBus.hasSubscribers());
    EventSubscription subscription = eventBus.subscribe(event -> {
    });
    assertTrue(eventBus.hasSubscribers());

    subscription.unsubscribe();
    assertFalse(eventBus.hasSubscribers());
  }

  @Test
  void receiveEvents() {
    DefaultEventBus eventBus = DefaultEventBus.create();

    AtomicInteger eventsReceived = new AtomicInteger();
    eventBus.subscribe(event -> eventsReceived.incrementAndGet());

    eventBus.start().block();

    assertEquals(EventBus.PublishResult.SUCCESS, eventBus.publish(mock(Event.class)));
    assertEquals(EventBus.PublishResult.SUCCESS, eventBus.publish(mock(Event.class)));
    assertEquals(EventBus.PublishResult.SUCCESS, eventBus.publish(mock(Event.class)));

    waitUntilCondition(() -> eventsReceived.get() == 3);

    eventBus.stop().block();
  }
}
