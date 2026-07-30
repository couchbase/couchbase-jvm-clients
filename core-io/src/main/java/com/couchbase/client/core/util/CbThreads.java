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

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Stability.Internal
public class CbThreads {
  private static final Logger log = LoggerFactory.getLogger(CbThreads.class);

  /**
   * Returns a new executor from {@link Executors#newThreadPerTaskExecutor(ThreadFactory)}
   * if virtual threads are available, otherwise {@link Executors#newCachedThreadPool()}
   * backed by platform daemon threads.
   *
   * @param threadNamePrefix including any trailing delimiters (for example, "my-pool-").
   */
  public static ExecutorService unboundedExecutorService(String threadNamePrefix) {
    ThreadFactory threadFactory = virtualThreadFactoryOrNull(threadNamePrefix);
    if (threadFactory != null) {
      try {
        return (ExecutorService) Executors.class.getMethod("newThreadPerTaskExecutor", ThreadFactory.class).invoke(null, threadFactory);
      } catch (ReflectiveOperationException | IllegalArgumentException | ClassCastException e) {
        // Shouldn't end up here, since newThreadPerTaskExecutor() was added at the same time as virtual threads.
        log.warn("Failed to invoke Executors.newThreadPerTaskExecutor. Falling back to Executors.newCachedThreadPool.", e);
      }
    }

    return Executors.newCachedThreadPool(platformThreadFactory(threadNamePrefix));
  }

  /**
   * @return null if the JVM does not support virtual threads
   */
  public static @Nullable ThreadFactory virtualThreadFactoryOrNull(String namePrefix) {
    try {
      Object virtualThreadBuilder = Thread.class.getMethod("ofVirtual").invoke(null);
      Class<?> builderClass = Class.forName("java.lang.Thread$Builder$OfVirtual");

      builderClass.getMethod("name", String.class, long.class)
        .invoke(virtualThreadBuilder, namePrefix, 0L);

      ThreadFactory result = (ThreadFactory) builderClass.getMethod("factory").invoke(virtualThreadBuilder);
      log.info("Using virtual threads for {}#", namePrefix);
      return result;

    } catch (ReflectiveOperationException | IllegalArgumentException | ClassCastException e) {
      log.debug("Failed to create virtual thread factory. This is normal prior to Java 21.", e);
      return null;
    }
  }

  public static ThreadFactory platformThreadFactory(String namePrefix) {
    log.info("Using platform threads for {}#", namePrefix);
    return new PlatformThreadFactory(namePrefix);
  }

  private static final class PlatformThreadFactory implements ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger();
    private final String namePrefix;

    public PlatformThreadFactory(String namePrefix) {
      this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName(namePrefix + counter.incrementAndGet());
      // Create daemon threads so we don't block the JVM from exiting if the user forgets cluster.disconnect()
      t.setDaemon(true);
      return t;
    }

    @Override
    public String toString() {
      return "PlatformThreadFactory{" +
        ", namePrefix='" + namePrefix + '\'' +
        '}';
    }
  }
}
