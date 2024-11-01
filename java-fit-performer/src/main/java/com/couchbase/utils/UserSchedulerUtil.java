/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.utils;

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UserSchedulerUtil {
  private UserSchedulerUtil() {
  }

  private static final Logger logger = LoggerFactory.getLogger(UserSchedulerUtil.class);

  private static final String USER_SCHEDULER_THREAD_POOL_NAME = "custom-user-scheduler";

  public record ExecutorAndScheduler(ExecutorService executorService, Scheduler scheduler) implements Disposable {
    @Override
    public void dispose() {
      scheduler.dispose();
      executorService.shutdownNow();
    }
  }

  public static ExecutorAndScheduler userExecutorAndScheduler() {
    var executorService = Executors.newCachedThreadPool(new DefaultThreadFactory(USER_SCHEDULER_THREAD_POOL_NAME, true));
    var userScheduler = Schedulers.fromExecutor(executorService);
    return new ExecutorAndScheduler(executorService, userScheduler);
  }

  // Getting stack traces is expensive; skip it in case this is a performance test.
  private static final boolean CAPTURE_STACK_TRACE = false;

  public static <T> Mono<T> withSchedulerCheck(Mono<T> publisher) {
    Exception location = CAPTURE_STACK_TRACE ? new Exception("Scheduler check was applied here") : null;
    return publisher
            .doOnNext(it -> assertInCustomUserSchedulerThread("onNext", location))
            .doOnError(it -> assertInCustomUserSchedulerThread("onError", location))
            .doOnSuccess(it -> assertInCustomUserSchedulerThread("onSuccess", location));
  }

  public static <T> Flux<T> withSchedulerCheck(Flux<T> publisher) {
    Exception location = CAPTURE_STACK_TRACE ? new Exception("Scheduler check was applied here") : null;
    return publisher
            .doOnNext(it -> assertInCustomUserSchedulerThread("onNext", location))
            .doOnError(it -> assertInCustomUserSchedulerThread("onError", location))
            .doOnComplete(() -> assertInCustomUserSchedulerThread("onComplete", location));
  }

  private static void assertInCustomUserSchedulerThread(
          String hookType,
          @Nullable Exception locationWhereSchedulerCheckWasApplied
  ) {
    // [if:3.7.5] first version that allows specifying custom publishOn scheduler
    String threadName = Thread.currentThread().getName();
    boolean isUserThread = threadName.contains(USER_SCHEDULER_THREAD_POOL_NAME);

    if (!isUserThread) {
      String location = locationWhereSchedulerCheckWasApplied == null
              ? "To discover the location of the failed scheduler check, set CAPTURE_STACK_TRACE to true in performer source code."
              : Throwables.getStackTraceAsString(locationWhereSchedulerCheckWasApplied);

      String msg = "Expected reactive " + hookType + " handler to run in custom user scheduler thread, but thread name was: " + threadName + " ; location = " + location;
      logger.error(msg);
      throw new InternalPerformerFailure(new RuntimeException(msg));
    }
    // [end]
  }
}
