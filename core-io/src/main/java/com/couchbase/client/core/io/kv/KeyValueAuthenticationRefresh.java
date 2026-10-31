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

package com.couchbase.client.core.io.kv;

import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.util.SynchronousEventBus;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A KV connection's subscription to reauthentication signals (see {@code Core.authenticationRefreshTriggers}).
 * The opener subscribes before connecting, so a signal during the handshake isn't missed. Signals before the
 * connection starts are remembered, and acted on once it does.
 */
@NullMarked
final class KeyValueAuthenticationRefresh {
  private static final Logger log = LoggerFactory.getLogger(KeyValueAuthenticationRefresh.class);

  private final SynchronousEventBus.Subscription subscription;

  // Guarded by "this".
  private @Nullable Runnable onSignal;
  private boolean signalled;

  KeyValueAuthenticationRefresh(EndpointContext endpointContext) {
    // Handled on another thread, so the publisher isn't held up, and closing the connection can unsubscribe.
    this.subscription = endpointContext.core().authenticationRefreshTriggers.subscribe(
      signal -> NioKeyValueEndpoint.tasks.execute(this::signal)
    );
  }

  private void signal() {
    Runnable action;
    synchronized (this) {
      action = onSignal;
      if (action == null) {
        log.info("Deferring reauthentication until handshake completes.");
        signalled = true;
        return;
      }
    }
    action.run();
  }

  /**
   * From now on, runs the action for each signal.
   *
   * @return true if there was a signal before now (which the caller should act on).
   */
  synchronized boolean start(Runnable onSignal) {
    this.onSignal = requireNonNull(onSignal);
    boolean result = signalled;
    signalled = false;
    return result;
  }

  void unsubscribe() {
    subscription.unsubscribe();
  }
}
