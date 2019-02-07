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

import org.jctools.queues.QueueFactory;
import org.jctools.queues.spec.ConcurrentQueueSpec;
import reactor.core.publisher.Mono;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
 * <p>Keep in mind to properly {@link #start()} and {@link #stop()} since it runs in its
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
   * Holds all current event subscribers.
   */
  private final CopyOnWriteArraySet<Consumer<Event>> subscribers;

  /**
   * Holds the bounded event mpsc queue dealing with all the events.
   */
  private final Queue<Event> eventQueue;

  /**
   * Contains the state if this event bus is currently running or not.
   */
  private final AtomicBoolean running;

  /**
   * What should be done if something needs to be messaged to the user because
   * of some failure.
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
   * If the event bus is running, this variable holds the thread.
   */
  private volatile Thread runningThread;

  public static DefaultEventBus.Builder builder() {
    return new Builder();
  }

  public static DefaultEventBus create() {
    return builder().build();
  }

  private DefaultEventBus(final Builder builder) {
    subscribers = new CopyOnWriteArraySet<>();
    running = new AtomicBoolean(false);

    eventQueue = QueueFactory.newQueue(
      ConcurrentQueueSpec.createBoundedMpsc(builder.queueCapacity)
    );
    errorLogging = builder.errorLogging.orElse(null);
    threadName = builder.threadName;
    idleSleepDuration = builder.idleSleepDuration;
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
    if (eventQueue.offer(event)) {
      return PublishResult.SUCCESS;
    } else {
      if (errorLogging != null) {
        errorLogging.println("Could not publish Event because the queue is full. " + event);
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
          while (isRunning()) {
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
                  }
                }
              }
              event = eventQueue.poll();
            }

            try {
              Thread.sleep(idleSleepTime);
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
   * Stops the {@link DefaultEventBus} from running.
   */
  @Override
  public Mono<Void> stop() {
    return Mono.defer(() -> {
      if(running.compareAndSet(true, false)) {
        runningThread.interrupt();
      }
      return Mono.empty();
    });
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

    int queueCapacity;
    Optional<PrintStream> errorLogging;
    String threadName;
    Duration idleSleepDuration;

    Builder() {
      queueCapacity = DEFAULT_QUEUE_CAPACITY;
      errorLogging = Optional.of(System.err);
      threadName = "cb-events";
      idleSleepDuration = DEFAULT_IDLE_SLEEP_DURATION;
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

    public DefaultEventBus build() {
      return new DefaultEventBus(this);
    }

  }
}
