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

import java.util.function.Consumer;

/**
 * The {@link EventBus} is the main demarcation line between internal behavior and external
 * visibility.
 *
 * <p>All events happening inside the system (and those fed from the outside as well) are visible
 * through the {@link EventBus}.</p>
 */
public interface EventBus {

  /**
   * Try to publish an event.
   *
   * @param event the event to publish.
   * @return the {@link PublishResult} of th event.
   */
  PublishResult publish(Event event);

  /**
   * Subscribes a {@link Consumer} to receive {@link Event Events}.
   *
   * @param consumer the consumer which will receive events.
   * @return a {@link EventSubscription} that can be used to unsubscribe.
   */
  EventSubscription subscribe(Consumer<Event> consumer);

  /**
   * Unsubscribes the {@link Consumer} from this {@link EventBus}.
   *
   * @param subscription the subscription used.
   */
  void unsubscribe(EventSubscription subscription);

  /**
   * Signals if a publish call was successful and if not why.
   */
  enum PublishResult {
    /**
     * Publishing was successful.
     */
    SUCCESS,

    /**
     * Could not publish because the event bus is overloaded temporarily.
     */
    OVERLOADED,
  }
}
