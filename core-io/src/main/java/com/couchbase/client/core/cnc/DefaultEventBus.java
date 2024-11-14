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

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.CbCollections;
import com.couchbase.client.core.util.NanoTimestamp;
import com.couchbase.client.core.util.NativeImageHelper;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.PrintStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.CbThrowables.getStackTraceAsString;

/**
 * The {@link DefaultEventBus} provides the default and very efficient implementation
 * of the {@link EventBus}.
 *
 * <p>It is built on top of a very fast, bounded MPSC (multi-producer single-consumer)
 * queue which allows to quickly send events into the bus without blocking the sender.</p>
 *
 * <p>Subscribers of this API are considered to be non-blocking and if they have to blocking
 * tasks need to fan them out into their own thread pool.</p>
 *
 * <p>Keep in mind to properly {@link #start()} and {@link #stop(Duration)} since it runs in its
 * own thread!</p>
 */
public class DefaultEventBus implements EventBus {

  /**
   * By default, the event bus can buffer up to 16k elements before it will fail to accept further
   * events.
   */
  private static final int DEFAULT_QUEUE_CAPACITY = 16384;

  /**
   * If there are no events to process, the event bus will sleep for 100ms before checking the queue
   * again.
   */
  private static final Duration DEFAULT_IDLE_SLEEP_DURATION = Duration.ofMillis(100);

  /**
   * Contains the default interval overflowed messages are logged.
   */
  private static final Duration DEFAULT_OVERFLOW_LOG_INTERVAL = Duration.ofSeconds(30);

  /**
   * Holds all current event subscribers.
   */
  private final CopyOnWriteArraySet<Consumer<Event>> subscribers = new CopyOnWriteArraySet<>();

  /**
   * Holds the bounded event mpsc queue dealing with all the events.
   */
  private final Queue<Event> eventQueue;

  /**
   * Contains the state if this event bus is currently running or not.
   */
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * If the event queue is full, this print stream will notify the user (if not null) of the dropped
   * event(s).
   */
  private final PrintStream errorLogging;

  /**
   * The name of the running thread.
   */
  private final String threadName;

  /**
   * The duration to sleep when no events are consumable.
   */
  private final Duration idleSleepDuration;

  /**
   * The interval at which overflowed events are printed.
   */
  private final Duration overflowLogInterval;

  /**
   * The scheduler used during i.e. shutdown.
   */
  private final Scheduler scheduler;

  /**
   * If the event bus is running, this variable holds the thread.
   */
  private volatile Thread runningThread;

  /**
   * The nano timestamp when the overflow log was last emitted.
   */
  private volatile NanoTimestamp overflowLogTimestamp = NanoTimestamp.never();

  /**
   * This maps stores one event per event class so that it can be printed on overflow but does not
   * spam the logs.
   */
  private final Map<Class<? extends Event>, SampleEventAndCount> overflowInfo = new ConcurrentHashMap<>();

  public static DefaultEventBus.Builder builder(final Scheduler scheduler) {
    return new Builder(scheduler);
  }

  public static DefaultEventBus create(final Scheduler scheduler) {
    return builder(scheduler).build();
  }

  private DefaultEventBus(final Builder builder) {
    eventQueue = NativeImageHelper.createMpscArrayQueue(builder.queueCapacity);
    scheduler = builder.scheduler;
    errorLogging = builder.errorLogging.orElse(null);
    threadName = builder.threadName;
    idleSleepDuration = builder.idleSleepDuration;
    overflowLogInterval = builder.overflowLogInterval;
  }

  @Override
  public EventSubscription subscribe(final Consumer<Event> consumer) {
    subscribers.add(consumer);
    return new EventSubscription(this, consumer);
  }

  @Override
  public void unsubscribe(final EventSubscription subscription) {
    subscribers.remove(subscription.consumer());
  }

  @Override
  public PublishResult publish(final Event event) {
    if (!isRunning()) {
      return PublishResult.SHUTDOWN;
    } else if (eventQueue.offer(event)) {
      return PublishResult.SUCCESS;
    } else {
      if (errorLogging != null) {
        try {
          overflowInfo.compute(
            event.getClass(),
            (k, v) -> v == null ? new SampleEventAndCount(event) : v.updateAndIncrement(event)
          );
        } catch (Exception ex) {
          // ignored. we failed on the overflow error handling - this should be very rare but
          // just in case it's important to move on and not let it impact the actual application
          // in this case.
        }
      }
      return PublishResult.OVERLOADED;
    }
  }

  /**
   * Starts the {@link DefaultEventBus}.
   */
  @Override
  public Mono<Void> start() {
    return Mono.defer(() -> {
      if (running.compareAndSet(false, true)) {
        runningThread = new Thread(() -> {
          long idleSleepTime = idleSleepDuration.toMillis();
          long overflowCounter = 0;
          while (isRunning() || !eventQueue.isEmpty()) {
            Event event = eventQueue.poll();
            while (event != null) {
              for (Consumer<Event> subscriber : subscribers) {
                try {
                  subscriber.accept(event);
                } catch (Throwable t) {
                  // any exception thrown in the event consumer is
                  // ignored, since it would otherwise kill the
                  // event bus thread!
                  if (errorLogging != null) {
                    errorLogging.println("Exception caught in EventBus Consumer: " + t);
                    t.printStackTrace();
                  }
                }
              }
              event = eventQueue.poll();

              // After processing 10_000 events, check if the overflow print needs
              // to be performed, to avoid the situation where a constant stream of
              // influx events (which is a contributing factor to overflow in the first
              // place) might never trigger the overflow print.
              if (++overflowCounter == 10_000) {
                maybePrintOverflow();
                overflowCounter = 0;
              }
            }

            maybePrintOverflow();

            try {
              if (isRunning()) {
                Thread.sleep(idleSleepTime);
              }
            } catch (InterruptedException e) {
              // If this thread is interrupted, we continue
              // into the loop early. so if interrupted for
              // shutdown it completes quickly while sleeping
            }
          }
        });

        runningThread.setDaemon(true);
        runningThread.setName(threadName);
        runningThread.start();
      }
      return Mono.empty();
    });
  }

  /**
   * Checks if the overflow log should be printed to error logging and performs the action if needed.
   */
  private void maybePrintOverflow() {
    try {
      if (errorLogging != null && !overflowInfo.isEmpty()) {
        if (overflowLogTimestamp.hasElapsed(overflowLogInterval)) {

          Map<String, Object> encodedEvents = new HashMap<>();
          for (Iterator<Map.Entry<Class<? extends Event>, SampleEventAndCount>> i = overflowInfo.entrySet().iterator(); i.hasNext(); ) {
            Map.Entry<Class<? extends Event>, SampleEventAndCount> e = i.next();
            encodedEvents.put(
              e.getKey().getSimpleName(),
              CbCollections.mapOf("sampleEvent", e.getValue().event.toString(), "totalDropCount", e.getValue().count)
            );
            i.remove();
          }
          errorLogging.println("Some events could not be published because the queue was (likely " +
            "temporarily) over capacity: " + Mapper.encodeAsString(encodedEvents));
          overflowInfo.clear();
          overflowLogTimestamp = NanoTimestamp.now();
        }
      }
    } catch (Exception ex) {
      errorLogging.println("Encountered an error while processing the overflow queue - this is a bug: " + getStackTraceAsString(ex));
      overflowInfo.clear();
    }
  }

  /**
   * Stops the {@link DefaultEventBus} from running.
   */
  @Override
  public Mono<Void> stop(final Duration timeout) {
    return Mono
      .defer(() -> {
        if(running.compareAndSet(true, false)) {
          runningThread.interrupt();
        }
        overflowInfo.clear();
        return Mono.empty();
      })
      .then(Flux.interval(Duration.ofMillis(10), scheduler).takeUntil(i -> !runningThread.isAlive()).then())
      .timeout(timeout, scheduler);
  }

  /**
   * True if running, false otherwise.
   */
  boolean isRunning() {
    return running.get();
  }

  /**
   * True if there are subscribers on the event bus right now.
   */
  boolean hasSubscribers() {
    return !subscribers.isEmpty();
  }

  /**
   * Allows to modify the default configuration of the {@link DefaultEventBus}.
   */
  public static class Builder {

    private final Scheduler scheduler;

    private int queueCapacity;
    private Optional<PrintStream> errorLogging;
    private String threadName;
    private Duration idleSleepDuration;
    private Duration overflowLogInterval;

    Builder(final Scheduler scheduler) {
      this.scheduler = scheduler;

      queueCapacity = DEFAULT_QUEUE_CAPACITY;
      errorLogging = Optional.of(System.err);
      threadName = "cb-events";
      idleSleepDuration = DEFAULT_IDLE_SLEEP_DURATION;
      overflowLogInterval = DEFAULT_OVERFLOW_LOG_INTERVAL;
    }

    public Builder queueCapacity(final int queueCapacity) {
      this.queueCapacity = queueCapacity;
      return this;
    }

    public Builder errorLogging(final Optional<PrintStream> errorLogging) {
      this.errorLogging = errorLogging;
      return this;
    }

    public Builder threadName(final String threadName) {
      this.threadName = threadName;
      return this;
    }

    public Builder idleSleepDuration(final Duration idleSleepDuration) {
      this.idleSleepDuration = idleSleepDuration;
      return this;
    }

    public Builder overflowLogInterval(final Duration overflowLogInterval) {
      this.overflowLogInterval = overflowLogInterval;
      return this;
    }

    public DefaultEventBus build() {
      return new DefaultEventBus(this);
    }

  }

  /**
   * Value object that encapsulates a sample event and the total drop count for each event class.
   */
  static class SampleEventAndCount {

    private volatile Event event;
    private final AtomicLong count = new AtomicLong(1);

    private SampleEventAndCount(Event event) {
      this.event = event;
    }

    /**
     * Updates the latest event and along the way increments the count by one.
     *
     * @param event the newest event to update.
     * @return the same {@link SampleEventAndCount}.
     */
    public SampleEventAndCount updateAndIncrement(Event event) {
      this.event = event;
      count.incrementAndGet();
      return this;
    }

    @Override
    public String toString() {
      return "{" +
        "sampleEvent=" + event +
        ", totalDropCount=" + count +
        '}';
    }
  }

}
