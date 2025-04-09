/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.apptelemetry.reporter;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.apptelemetry.collector.AppTelemetryCollector;
import com.couchbase.client.core.env.CouchbaseThreadFactory;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import static com.couchbase.client.core.logging.RedactableArgument.redactSystem;
import static com.couchbase.client.core.util.CbCollections.setCopyOf;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Stability.Internal
public class AppTelemetryReporterImpl implements AppTelemetryReporter {
  private static final Logger log = LoggerFactory.getLogger(AppTelemetryReporterImpl.class);

  private static final ThreadFactory threadFactory = new CouchbaseThreadFactory("app-telemetry-reporter-");
  private static final BackoffCalculator backoffCalculator = new BackoffCalculator(Duration.ofMillis(100), Duration.ofHours(1));

  private final AtomicLong connectionAttempt = new AtomicLong();
  private final AppTelemetryCollector collector;
  private final Thread thread;
  private volatile boolean done; // guarded by "this"

  private Set<URI> remotes = emptySet(); // guarded by "this"
  private @Nullable URI selectedRemote;

  public AppTelemetryReporterImpl(
    CoreContext ctx,
    AppTelemetryCollector collector
  ) {
    this.collector = requireNonNull(collector);

    AppTelemetryWebSocketClient webSocketClient = new AppTelemetryWebSocketClient(ctx, collector);

    // The worker thread spends most of its time blocking,
    // waiting for at least one eligible host to appear,
    // waiting for the channel to connect and then close,
    // waiting for the connected host to leave the cluster, etc.
    this.thread = threadFactory.newThread((() -> {
      try {
        while (!done) {
          try {
            sleepForBackoff(connectionAttempt.getAndIncrement());

            URI remote = selectRemote();
            if (remote == null) {
              sleepUntilInterrupted();
              throw new RuntimeException("unreachable");
            }

            webSocketClient.connectAndWaitForClose(remote, this::resetBackoff);
            log.info("App telemetry connection closed by peer; recalibrating!");

          } catch (InterruptedException e) {
            resetBackoff();
            log.info("App telemetry reporter interrupted; recalibrating!");

          } catch (Exception e) {
            log.info("App telemetry connection failed; recalibrating!", e);
          }
        }
      } finally {
        log.info("App telemetry reporter thread finished.");
      }
    }));
    this.thread.start();
  }

  private void resetBackoff() {
    connectionAttempt.set(0);
  }

  private static void sleepUntilInterrupted() throws InterruptedException {
    Thread.sleep(Long.MAX_VALUE);
  }

  private void sleepForBackoff(long attempt) throws InterruptedException {
    Duration delay = backoffCalculator.delayForAttempt(attempt);
    log.debug("App telemetry reporter connection attempt {} sleeping for backoff: {}", attempt, delay);
    MILLISECONDS.sleep(delay.toMillis());
  }

  private synchronized @Nullable URI selectRemote() {
    selectedRemote = randomOrNull(this.remotes);
    if (selectedRemote == null) {
      log.info("App telemetry reporter has no remotes available.");
    } else {
      log.info("Selected remote for app telemetry: {}", redactSystem(selectedRemote));
    }
    return selectedRemote;
  }

  @Override
  public synchronized void updateRemotes(Set<URI> updatedRemotes) {
    collector.setPaused(done || updatedRemotes.isEmpty());

    if (done || updatedRemotes.equals(this.remotes)) {
      return;
    }

    this.remotes = setCopyOf(updatedRemotes);

    if (selectedRemote == null && !remotes.isEmpty()) {
      interruptWorker("a remote host is now available");

    } else if (selectedRemote != null && !remotes.contains(selectedRemote)) {
      selectedRemote = null;
      interruptWorker("the previously selected remote host no longer wants to receive telemetry");
    }
  }

  @Override
  public synchronized void close() {
    if (!done) {
      done = true;
      interruptWorker("the reporter is shutting down");
    }
  }

  private void interruptWorker(String reason) {
    log.info("Interrupting the app telemetry reporter thread because {}", reason);
    thread.interrupt();
  }

  private static final Random random = new Random();

  private static <T> @Nullable T randomOrNull(Collection<T> items) {
    if (items.isEmpty()) {
      return null;
    }

    int index = random.nextInt(items.size());

    if (items instanceof List) {
      return ((List<T>) items).get(index);
    }

    Iterator<T> iterator = items.iterator();
    for (int i = 0; i < index; i++) {
      iterator.next();
    }

    return iterator.next();
  }
}
