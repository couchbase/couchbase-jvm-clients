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
 * Subscription for a consumer on a {@link EventBus} that can be used
 * to unsubscribe once done.
 */
public class EventSubscription {

    /**
     * The event bus that this subscription is connected to.
     */
    private final EventBus eventBus;

    /**
     * The consumer consuming the events.
     */
    private final Consumer<Event> consumer;

    /**
     * Creates a new {@link EventSubscription}.
     *
     * @param eventBus The event bus that this subscription is connected to.
     * @param consumer The consumer consuming the events.
     */
    EventSubscription(final EventBus eventBus, final Consumer<Event> consumer) {
        this.eventBus = eventBus;
        this.consumer = consumer;
    }

    /**
     * The consumer consuming the events.
     */
    Consumer<Event> consumer() {
        return consumer;
    }

    /**
     * Allows to unsubscribe the {@link Consumer} from the {@link EventBus}.
     */
    public void unsubscribe() {
        eventBus.unsubscribe(this);
    }
}
