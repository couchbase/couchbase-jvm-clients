/*
 * Copyright 2023 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.EventBus.PublishResult;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ClusterCleanupTask implements Runnable {

  private static class ClusterLeakDetected extends AbstractEvent {
    private final Throwable cause;

    protected ClusterLeakDetected(Throwable cause) {
      super(Severity.WARN, Category.CORE, Duration.ZERO, null);
      this.cause = requireNonNull(cause);
    }

    @Override
    public String description() {
      return cause.getMessage();
    }

    @Override
    public Throwable cause() {
      return cause;
    }
  }

  private final AtomicBoolean alreadyDisconnected;
  private final EventBus eventBus;
  private final Mono<Void> cleanupTask;

  private final Exception clusterCreationStackTrace = new Exception(
    "A Couchbase `Cluster` object became unreachable (eligible for garbage collection) without first being disconnected." +
      " As a safeguard, it is being automatically disconnected now." +
      " Please correct this problem by calling `Cluster.disconnect()` or `Cluster.close()` when you are done with the Cluster object" +
      " and all associated Buckets, Scopes, Collections, managers, etc." +
      " The stack trace points to the location where the leaked Cluster was created."
  );

  public ClusterCleanupTask(
    Mono<Void> cleanupTask,
    EventBus eventBus,
    AtomicBoolean alreadyDisconnected
  ) {
    this.alreadyDisconnected = requireNonNull(alreadyDisconnected);
    this.cleanupTask = requireNonNull(cleanupTask);
    this.eventBus = eventBus;
  }

  @Override
  public void run() {
    if (alreadyDisconnected.get()) {
      return;
    }

    if (PublishResult.SUCCESS != eventBus.publish(new ClusterLeakDetected(clusterCreationStackTrace))) {
      clusterCreationStackTrace.printStackTrace();
    }

    cleanupTask.block();
  }
}
