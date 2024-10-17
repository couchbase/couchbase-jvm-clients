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

package com.couchbase.client.core.util;


import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import reactor.core.CorePublisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@Stability.Internal
public interface ReactorOps {

  default <T> Mono<T> publishOnUserScheduler(Supplier<CompletableFuture<T>> future) {
    return publishOnUserScheduler(Reactor.toMono(future));
  }

  <T> Mono<T> publishOnUserScheduler(Mono<T> mono);

  <T> Flux<T> publishOnUserScheduler(Flux<T> mono);

  /**
   * Returns a dynamic proxy for the given object
   * (unless the given supplier is null, in which case
   * the same object is returned).
   * <p>
   * Any Flux or Mono instances returned by the proxied interface methods
   * are published on the scheduler returned by the given supplier.
   */
  @Stability.Internal
  static <T> T proxyToPublishOnSuppliedScheduler(
    T obj,
    Class<T> interfaceToProxy,
    @Nullable Supplier<Scheduler> scheduler
  ) {
    if (scheduler == null) {
      return obj;
    }
    return interfaceToProxy.cast(
      Proxy.newProxyInstance(
        interfaceToProxy.getClassLoader(),
        new Class[]{interfaceToProxy},
        new InvocationHandler() {
          @SuppressWarnings("ReactiveStreamsUnusedPublisher")
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
              Object result = method.invoke(obj, args);
              if (result instanceof CorePublisher) {
                if (result instanceof Mono) {
                  return Mono.defer(() -> ((Mono<?>) result).publishOn(scheduler.get()));
                }
                if (result instanceof Flux) {
                  return Flux.defer(() -> ((Flux<?>) result).publishOn(scheduler.get()));
                }
              }
              return result;
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
          }
        })
    );
  }
}
