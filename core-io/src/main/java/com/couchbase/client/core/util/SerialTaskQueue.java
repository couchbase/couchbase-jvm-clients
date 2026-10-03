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
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Runs asynchronous tasks one at a time, in submission order.
 * <p>
 * A task is a {@code Supplier<Mono<Void>>}. The next task does not start
 * until the {@code Mono} returned by the previous task terminates.
 * Tasks run on the given scheduler.
 * <p>
 * Cancelling the {@code Mono} returned by {@link #submit} before the task
 * starts means the task is skipped. Cancelling after the task starts has
 * no effect on the task; it still runs to completion before the next task starts.
 * <p>
 * A task must not wait for the completion of another task submitted to the same queue,
 * or it will deadlock. Attempting to do so fails with {@link IllegalStateException}.
 * <p>
 * If the scheduler rejects work (for example, because it was disposed),
 * every task waiting in the queue fails with the scheduler's exception
 * instead of waiting forever. A task already running is not affected.
 * <p>
 * Each task's Mono is subscribed with a Reactor {@link Context} that identifies this queue.
 * Code that must only run as part of a task can verify this with {@link #requireInTask()}.
 */
@Stability.Internal
public final class SerialTaskQueue {

  private final Scheduler scheduler;
  private final Queue<Task> queue = new ConcurrentLinkedQueue<>();

  /**
   * True while a task is running, or while a drain is scheduled.
   */
  private final AtomicBoolean active = new AtomicBoolean();

  /**
   * Context key whose value is this queue, present only in the Context of this queue's tasks.
   */
  private final Object contextKey = new Object();

  public SerialTaskQueue(Scheduler scheduler) {
    this.scheduler = requireNonNull(scheduler);
  }

  /**
   * Returns a Mono that, when subscribed, enqueues the task and completes
   * (or fails) when the task's Mono terminates.
   */
  public Mono<Void> submit(Supplier<Mono<Void>> work) {
    requireNonNull(work);
    return Mono.deferContextual(ctx -> {
      if (isTaskContext(ctx)) {
        return Mono.error(new IllegalStateException(
          "A task must not wait for another task submitted to the same SerialTaskQueue; it would deadlock."
        ));
      }

      Task task = new Task(work);
      queue.add(task);
      scheduleDrainIfIdle();
      return task.done.asMono().doOnCancel(() -> task.cancelled = true);
    });
  }

  /**
   * Returns a Mono that completes if it is part of a task running on this queue,
   * or fails with {@link IllegalStateException} otherwise.
   * <p>
   * The check uses the Reactor Context, so it works regardless of which thread runs the task.
   */
  public Mono<Void> requireInTask() {
    return Mono.deferContextual(ctx -> isTaskContext(ctx)
      ? Mono.empty()
      : Mono.error(new IllegalStateException("Must only be called from a task running on this SerialTaskQueue."))
    );
  }

  private boolean isTaskContext(ContextView ctx) {
    return ctx.getOrDefault(contextKey, null) == this;
  }

  private void scheduleDrainIfIdle() {
    if (active.compareAndSet(false, true)) {
      scheduleRunNext();
    }
  }

  /**
   * Schedules {@link #runNext()}. Caller must have set {@link #active}.
   * <p>
   * If the scheduler rejects it, clears {@link #active} and fails all queued tasks,
   * so nobody waits for a drain that will never happen.
   */
  private void scheduleRunNext() {
    try {
      scheduler.schedule(this::runNext);
    } catch (Throwable t) {
      // Clear `active` before draining. A task added after this point either gets drained below,
      // or its submitter wins the CAS in scheduleDrainIfIdle and tries scheduling for itself.
      active.set(false);
      failQueuedTasks(t);
    }
  }

  private void failQueuedTasks(Throwable cause) {
    Task task;
    while ((task = queue.poll()) != null) {
      task.done.tryEmitError(cause);
    }
  }

  private void runNext() {
    Task task;
    do {
      task = queue.poll();
    } while (task != null && task.cancelled);

    if (task == null) {
      active.set(false);
      // A task might have been added after poll() returned null, but before `active` was cleared.
      if (!queue.isEmpty()) {
        scheduleDrainIfIdle();
      }
      return;
    }

    final Task current = task;
    Mono<Void> mono;
    try {
      mono = requireNonNull(current.work.get(), "task returned null Mono");
    } catch (Throwable t) {
      mono = Mono.error(t);
    }

    mono.contextWrite(Context.of(contextKey, this)).subscribe(
      ignored -> {
      },
      error -> {
        try {
          scheduleRunNext();
        } finally {
          current.done.tryEmitError(error);
        }
      },
      () -> {
        try {
          scheduleRunNext();
        } finally {
          current.done.tryEmitEmpty();
        }
      }
    );
  }

  private static final class Task {
    private final Supplier<Mono<Void>> work;
    private final Sinks.Empty<Void> done = Sinks.empty();
    private volatile boolean cancelled;

    Task(Supplier<Mono<Void>> work) {
      this.work = work;
    }
  }
}
