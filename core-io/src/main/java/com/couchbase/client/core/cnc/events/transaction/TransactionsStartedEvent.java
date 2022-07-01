/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.cnc.events.transaction;

import com.couchbase.client.core.annotation.Stability;

/**
 * An event that's fired when transactions are successfully initialised.  The main intent is for the user
 * to be able to verify that the event bus logging is configured correctly.
 */
@Stability.Uncommitted
public class TransactionsStartedEvent extends TransactionEvent {
  private final boolean runLostAttemptsCleanupThread;
  private final boolean runRegularAttemptsCleanupThread;

  public TransactionsStartedEvent(boolean runLostAttemptsCleanupThread, boolean runRegularAttemptsCleanupThread) {
    super(Severity.INFO, DEFAULT_CATEGORY);
    this.runLostAttemptsCleanupThread = runLostAttemptsCleanupThread;
    this.runRegularAttemptsCleanupThread = runRegularAttemptsCleanupThread;
  }

  @Override
  public String description() {
    return "Transactions successfully initialised, regular cleanup enabled="
            + runRegularAttemptsCleanupThread + ", lost cleanup enabled=" + runLostAttemptsCleanupThread;
  }

  public boolean runningLostAttemptsCleanupThread() {
    return runLostAttemptsCleanupThread;
  }

  public boolean runningRegularAttemptsCleanupThread() {
    return runRegularAttemptsCleanupThread;
  }
}
