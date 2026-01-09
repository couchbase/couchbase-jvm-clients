/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class SynchronousEventBus<T> {
  private static final Logger log = LoggerFactory.getLogger(SynchronousEventBus.class);

  public interface Subscription {
    void unsubscribe();
  }

  private class SubscriptionImpl implements Subscription {
    private final Consumer<T> callback;

    public SubscriptionImpl(Consumer<T> listener) {
      this.callback = requireNonNull(listener);
    }

    public void unsubscribe() {
      remove(callback);
    }
  }

  /**
   * @implNote Guarded by "this"
   */
  private final List<Consumer<T>> listeners = new ArrayList<>();

  public synchronized Subscription subscribe(Consumer<T> listener) {
    requireNonNull(listener);
    listeners.add(listener);
    return new SubscriptionImpl(listener);
  }

  private synchronized void remove(Consumer<T> listener) {
    listeners.remove(listener);
  }

  public synchronized void publish(T event) {
    for (Consumer<T> listener : listeners) {
      try {
        listener.accept(event);
      } catch (RuntimeException t) {
        log.error("SynchronousEventBus subscriber threw exception.", t);
      }
    }
  }
}
