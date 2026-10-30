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

package com.couchbase.client.core.service;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.NullMarked;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.unmodifiableList;

/**
 * The event loops for KV connections (see {@link KeyValueEventLoop} and {@link NioKeyValueConnection}).
 * Each connection gets the next loop in turn.
 * <p>
 * Owned by an environment (see {@link com.couchbase.client.core.env.IoEnvironment#nioKeyValueEventLoopGroup}),
 * which sizes it, and shuts it down.
 * <p>
 * Thread-safe.
 */
@Stability.Internal
@NullMarked
public final class KeyValueEventLoopGroup {
  /**
   * Numbers each group, for its threads' names.
   */
  private static final AtomicInteger groupIds = new AtomicInteger();

  private final List<KeyValueEventLoop> loops;
  private final AtomicInteger nextLoop = new AtomicInteger();
  private volatile boolean shutdown;

  /**
   * Starts the loops' threads, named "cb-kv-nio-{group}-{loop}".
   */
  public KeyValueEventLoopGroup(int threadCount) {
    if (threadCount < 1) {
      throw new IllegalArgumentException("threadCount must be positive, but was " + threadCount);
    }
    int groupId = groupIds.incrementAndGet();

    List<KeyValueEventLoop> tempLoops = new ArrayList<>(threadCount);
    try {
      for (int i = 0; i < threadCount; i++) {
        tempLoops.add(new KeyValueEventLoop("cb-kv-nio-" + groupId + "-" + i));
      }
    } catch (Throwable t) {
      tempLoops.forEach(KeyValueEventLoop::shutdown);
      throw t;
    }
    this.loops = unmodifiableList(tempLoops);
  }

  int threadCount() {
    return loops.size();
  }

  /**
   * Returns the loop for a new connection.
   *
   * @throws IllegalStateException if the group is shut down.
   */
  KeyValueEventLoop next() {
    if (shutdown) {
      throw new IllegalStateException("The KV event loops are shut down");
    }
    return loops.get(Math.floorMod(nextLoop.getAndIncrement(), loops.size()));
  }

  /**
   * Shuts down the loops: closes their connections, then stops their threads.
   *
   * @return a Mono that completes once the threads have stopped, or the timeout passes.
   */
  public Mono<Void> shutdown(Duration timeout) {
    return Mono.<Void>fromRunnable(() -> {
        shutdown = true;
        loops.forEach(KeyValueEventLoop::shutdown);
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
          for (KeyValueEventLoop loop : loops) {
            if (!loop.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
              return;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      })
      .subscribeOn(Schedulers.boundedElastic());
  }

  @Override
  public String toString() {
    return "KeyValueEventLoopGroup{loops=" + loops.size() + ", shutdown=" + shutdown + "}";
  }
}
