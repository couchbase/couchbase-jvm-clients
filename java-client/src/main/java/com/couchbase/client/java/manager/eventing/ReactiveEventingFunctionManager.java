/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions.getAllFunctionsOptions;

@Stability.Uncommitted
public class ReactiveEventingFunctionManager {

  private final AsyncEventingFunctionManager asyncManager;

  public ReactiveEventingFunctionManager(AsyncEventingFunctionManager asyncManager) {
    this.asyncManager = asyncManager;
  }

  public Mono<Void> upsertFunction(final EventingFunction function) {
    return upsertFunction(function, UpsertFunctionOptions.upsertFunctionOptions());
  }

  public Mono<Void> upsertFunction(final EventingFunction function, final UpsertFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.upsertFunction(function, options));
  }

  public Mono<EventingFunction> getFunction(final String name) {
    return getFunction(name, GetFunctionOptions.getFunctionOptions());
  }

  public Mono<EventingFunction> getFunction(final String name, final GetFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.getFunction(name, options));
  }

  public Mono<Void> dropFunction(final String name) {
    return dropFunction(name, DropFunctionOptions.dropFunctionOptions());
  }

  public Mono<Void> dropFunction(final String name, final DropFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.dropFunction(name, options));
  }

  public Mono<Void> deployFunction(final String name) {
    return deployFunction(name, DeployFunctionOptions.deployFunctionOptions());
  }

  public Mono<Void> deployFunction(final String name, final DeployFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.deployFunction(name, options));
  }

  public Flux<EventingFunction> getAllFunctions() {
    return getAllFunctions(getAllFunctionsOptions());
  }
  public Flux<EventingFunction> getAllFunctions(final GetAllFunctionsOptions options) {
    return Reactor.toMono(() -> asyncManager.getAllFunctions(options)).flatMapMany(Flux::fromIterable);
  }

  public Mono<Void> pauseFunction(final String name) {
    return pauseFunction(name, PauseFunctionOptions.pauseFunctionOptions());
  }

  public Mono<Void> pauseFunction(final String name, final PauseFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.pauseFunction(name, options));
  }

  public Mono<Void> resumeFunction(final String name) {
    return resumeFunction(name, ResumeFunctionOptions.resumeFunctionOptions());
  }

  public Mono<Void> resumeFunction(final String name, final ResumeFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.resumeFunction(name, options));
  }

  public Mono<Void> undeployFunction(final String name) {
    return undeployFunction(name, UndeployFunctionOptions.undeployFunctionOptions());
  }

  public Mono<Void> undeployFunction(final String name, final UndeployFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.undeployFunction(name, options));
  }
}
