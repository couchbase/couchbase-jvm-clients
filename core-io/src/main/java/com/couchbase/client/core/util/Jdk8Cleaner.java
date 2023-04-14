/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.env.CouchbaseThreadFactory;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;

/**
 * Approximates Java 9's <a href="https://docs.oracle.com/javase/9/docs/api/java/lang/ref/Cleaner.html">
 * java.lang.ref.Cleaner
 * </a>
 */
@Stability.Internal
public class Jdk8Cleaner {
  private static final ThreadFactory cleanerThreadFactory = new CouchbaseThreadFactory("cb-cleaner-");

  private final ReferenceQueue<Object> queue = new ReferenceQueue<>();

  // Only purpose is to prevent its contents from being prematurely garbage collected.
  private final Set<CleanableImpl> references = newSetFromMap(new ConcurrentHashMap<>());

  public static Jdk8Cleaner create(ThreadFactory factory) {
    return new Jdk8Cleaner(factory);
  }

  // visible for testing
  static AtomicInteger activeCleanerThreadCount = new AtomicInteger();

  /**
   * Creates a new cleaner, and registers the given cleanup action to run
   * when the given object becomes phantom reachable.
   * <p>
   * The cleaner's thread terminates after the given cleanup task is executed.
   * <p>
   * The cleaning action should generally not be a lambda, since it's easy to accidentally
   * capture a reference to the object, preventing it from ever becoming phantom reachable.
   */
  public static void registerWithOneShotCleaner(Object obj, Runnable cleanupTask) {
    Jdk8Cleaner cleaner = Jdk8Cleaner.create(cleanerThreadFactory);
    cleaner.register(obj, new OneShotCleanerAction(cleanupTask, cleaner.thread, cleaner.done));
  }

  private final Thread thread;
  private final AtomicBoolean done = new AtomicBoolean();

  private Jdk8Cleaner(ThreadFactory factory) {
    thread = factory.newThread(this::doRun);
    thread.start();
  }

  /**
   * Executes the given cleaning action when the object becomes phantom reachable.
   * <p>
   * The cleaning action should generally not be a lambda, since it's easy to accidentally
   * capture a reference to the object, preventing it from ever becoming phantom reachable.
   */
  public Cleanable register(Object obj, Runnable cleaningAction) {
    CleanableImpl cleanable = new CleanableImpl(obj, queue, cleaningAction);
    references.add(cleanable);
    return cleanable;
  }

  private void doRun() {
    activeCleanerThreadCount.incrementAndGet();

    try {
      while (!done.get()) {
        try {
          CleanableImpl r = (CleanableImpl) queue.remove();
          references.remove(r);
          r.clean();

        } catch (InterruptedException e) {
          return;
        }
      }
    } finally {
      activeCleanerThreadCount.decrementAndGet();
    }
  }

  private static class OneShotCleanerAction implements Runnable {
    private final Runnable wrappedAction;
    private final Thread thread;
    private final AtomicBoolean done;

    public OneShotCleanerAction(Runnable wrappedAction, Thread thread, AtomicBoolean done) {
      this.wrappedAction = requireNonNull(wrappedAction);
      this.thread = requireNonNull(thread);
      this.done = requireNonNull(done);
    }

    @Override
    public void run() {
      try {
        wrappedAction.run();
      } finally {
        done.set(true);
        thread.interrupt();
      }
    }
  }

  /**
   * An object and a cleaning action registered in a Cleaner.
   */
  public interface Cleanable {
    /**
     * Unregisters the cleanable and invokes the cleaning action.
     * The cleanable's cleaning action is invoked at most once
     * regardless of the number of calls to clean.
     */
    void clean();
  }

  private static class CleanableImpl extends PhantomReference<Object> implements Cleanable {
    private final Runnable cleaningAction;
    private final AtomicBoolean alreadyCleaned = new AtomicBoolean();

    CleanableImpl(Object referent, ReferenceQueue<Object> q, Runnable cleaningAction) {
      super(referent, q);
      this.cleaningAction = requireNonNull(cleaningAction);
    }

    @Override
    public void clean() {
      if (alreadyCleaned.compareAndSet(false, true)) {
        try {
          cleaningAction.run();
        } catch (Throwable t) {
          // Wish we could log this, but would need SLF4J. Revisit after we embrace SLF4J in SDK 3.5.
          t.printStackTrace();
        }
      }
    }
  }
}
