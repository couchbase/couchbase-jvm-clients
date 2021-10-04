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

import com.couchbase.client.core.cnc.events.io.SelectBucketFailedEvent;
import com.couchbase.client.core.cnc.events.io.WriteTrafficCapturedEvent;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.test.Util.threadRunning;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static com.couchbase.client.util.Assertions.assertThreadNotRunning;
import static com.couchbase.client.util.Assertions.assertThreadRunning;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies the functionality of the {@link DefaultEventBus}.
 */
class DefaultEventBusTest {

  @Test
  void startAndStopEventBus() {
    final String threadName = UUID.randomUUID().toString();

    DefaultEventBus eventBus = DefaultEventBus
      .builder(Schedulers.parallel())
      .threadName(threadName)
      .build();
    assertFalse(eventBus.isRunning());
    assertThreadNotRunning(threadName);

    eventBus.start().block();
    assertTrue(eventBus.isRunning());
    assertThreadRunning(threadName);

    eventBus.stop(Duration.ofSeconds(5)).block();
    waitUntilCondition(() -> !threadRunning(threadName));
    assertFalse(eventBus.isRunning());
    assertThreadNotRunning(threadName);
  }

  @Test
  void subscribeAndUnsubscribeFromEventBus() {
    DefaultEventBus eventBus = DefaultEventBus.create(Schedulers.parallel());

    assertFalse(eventBus.hasSubscribers());
    EventSubscription subscription = eventBus.subscribe(event -> {
    });
    assertTrue(eventBus.hasSubscribers());

    subscription.unsubscribe();
    assertFalse(eventBus.hasSubscribers());
  }

  @Test
  void receiveEvents() {
    DefaultEventBus eventBus = DefaultEventBus.create(Schedulers.parallel());

    AtomicInteger eventsReceived = new AtomicInteger();
    eventBus.subscribe(event -> eventsReceived.incrementAndGet());

    eventBus.start().block();

    assertEquals(EventBus.PublishResult.SUCCESS, eventBus.publish(mock(Event.class)));
    assertEquals(EventBus.PublishResult.SUCCESS, eventBus.publish(mock(Event.class)));
    assertEquals(EventBus.PublishResult.SUCCESS, eventBus.publish(mock(Event.class)));

    waitUntilCondition(() -> eventsReceived.get() == 3);

    eventBus.stop(Duration.ofSeconds(5)).block();
  }

  @Test
  void shutsDownOnlyOnceAllEventsConsumed() {
    DefaultEventBus eventBus = DefaultEventBus.create(Schedulers.parallel());
    eventBus.start().block();

    int eventsSent = 1000;
    AtomicInteger eventsReceived = new AtomicInteger();
    eventBus.subscribe(event -> eventsReceived.incrementAndGet());

    for (int i = 0; i < eventsSent; i++) {
      eventBus.publish(mock(Event.class));
    }

    eventBus.stop(Duration.ofSeconds(5)).block();
    assertEquals(eventsReceived.get(), eventsSent);
  }

  @Test
  void bundlesAndEmitsOverflow() {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);

    DefaultEventBus eventBus = DefaultEventBus
      .builder(Schedulers.parallel())
      .overflowLogInterval(Duration.ofSeconds(1))
      .queueCapacity(1)
      .errorLogging(Optional.of(ps))
      .build();
    eventBus.start().block();

    Event ev1 = mock(Event.class);
    SelectBucketFailedEvent ev2 = mock(SelectBucketFailedEvent.class);
    when(ev2.toString()).thenReturn("sbfe");
    WriteTrafficCapturedEvent ev3 = mock(WriteTrafficCapturedEvent.class);
    when(ev3.toString()).thenReturn("wtce");
    WriteTrafficCapturedEvent ev4 = mock(WriteTrafficCapturedEvent.class);
    when(ev4.toString()).thenReturn("wtce2");

    eventBus.publish(ev1);
    eventBus.publish(ev2);
    eventBus.publish(ev3);
    eventBus.publish(ev4);

    AtomicReference<String> emitted = new AtomicReference<>();
    waitUntilCondition(() -> {
      String e = os.toString();
      if (e.contains("Some events could not be published")) {
        emitted.set(e);
        return true;
      }
      return false;
    });

    assertTrue(emitted.get().contains("SelectBucketFailedEvent"));
    assertTrue(emitted.get().contains("WriteTrafficCapturedEvent"));
    assertTrue(emitted.get().contains("sbfe"));
    assertTrue(emitted.get().contains("wtce2"));

    eventBus.stop(Duration.ofSeconds(5)).block();
  }

}
