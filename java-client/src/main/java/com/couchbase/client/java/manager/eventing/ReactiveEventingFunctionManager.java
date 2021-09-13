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
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.EventingFunctionCompilationFailureException;
import com.couchbase.client.core.error.EventingFunctionDeployedException;
import com.couchbase.client.core.error.EventingFunctionIdenticalKeyspaceException;
import com.couchbase.client.core.error.EventingFunctionNotBootstrappedException;
import com.couchbase.client.core.error.EventingFunctionNotDeployedException;
import com.couchbase.client.core.error.EventingFunctionNotFoundException;
import com.couchbase.client.java.ReactiveCluster;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions.getAllFunctionsOptions;

/**
 * Performs management operations on {@link EventingFunction EventingFunctions}.
 */
@Stability.Uncommitted
public class ReactiveEventingFunctionManager {

  /**
   * The underlying async function manager which performs the actual ops and does the conversions.
   */
  private final AsyncEventingFunctionManager asyncManager;

  /**
   * Creates a new {@link ReactiveEventingFunctionManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link ReactiveCluster#eventingFunctions()}
   * instead.
   *
   * @param asyncManager the underlying async manager that performs the ops.
   */
  @Stability.Internal
  public ReactiveEventingFunctionManager(AsyncEventingFunctionManager asyncManager) {
    this.asyncManager = asyncManager;
  }

  /**
   * Inserts or replaces a {@link EventingFunction}.
   * <p>
   * The eventing management API defines that if a function is stored which name does not exist yet,
   * it will be inserted. If the name already exists, the function will be replaced with its new equivalent and
   * the properties changed.
   * <p>
   * Operations which change the runtime-state of a function (i.e. deploy / undeploy / pause / resume) should not
   * be modified through this method, but rather by using those methods directly (i.e. {@link #deployFunction(String)}).
   *
   * @param function the function to be inserted or replaced.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionCompilationFailureException (async) if the function body cannot be compiled.
   * @throws CollectionNotFoundException (async) if the specified collection or scope does not exist.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws EventingFunctionIdenticalKeyspaceException (async) if the source and metadata keyspace are the same.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> upsertFunction(final EventingFunction function) {
    return upsertFunction(function, UpsertFunctionOptions.upsertFunctionOptions());
  }

  /**
   * Inserts or replaces a {@link EventingFunction} with custom options.
   * <p>
   * The eventing management API defines that if a function is stored which name does not exist yet,
   * it will be inserted. If the name already exists, the function will be replaced with its new equivalent and
   * the properties changed.
   * <p>
   * Operations which change the runtime-state of a function (i.e. deploy / undeploy / pause / resume) should not
   * be modified through this method, but rather by using those methods directly (i.e. {@link #deployFunction(String)}).
   *
   * @param function the function to be inserted or replaced.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionCompilationFailureException (async) if the function body cannot be compiled.
   * @throws CollectionNotFoundException (async) if the specified collection or scope does not exist.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws EventingFunctionIdenticalKeyspaceException (async) if the source and metadata keyspace are the same.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> upsertFunction(final EventingFunction function, final UpsertFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.upsertFunction(function, options));
  }

  /**
   * Retrieves a {@link EventingFunction} by its name.
   *
   * @param name the name of the function to retrieve.
   * @return a {@link Mono} completing with the eventing function found or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<EventingFunction> getFunction(final String name) {
    return getFunction(name, GetFunctionOptions.getFunctionOptions());
  }

  /**
   * Retrieves a {@link EventingFunction} by its name with custom options.
   *
   * @param name the name of the function to retrieve.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing with the eventing function found or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<EventingFunction> getFunction(final String name, final GetFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.getFunction(name, options));
  }

  /**
   * Retrieves all {@link EventingFunction EventingFunctions} currently stored on the server.
   * <p>
   * If no functions are found, an empty flux is returned.
   *
   * @return a {@link Flux} completing with all eventing functions found or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<EventingFunction> getAllFunctions() {
    return getAllFunctions(getAllFunctionsOptions());
  }

  /**
   * Retrieves all {@link EventingFunction EventingFunctions} currently stored on the server with custom options.
   * <p>
   * If no functions are found, an empty flux is returned.
   *
   * @param options the custom options to apply.
   * @return a {@link Flux} completing with all eventing functions found or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Flux<EventingFunction> getAllFunctions(final GetAllFunctionsOptions options) {
    return Reactor.toMono(() -> asyncManager.getAllFunctions(options)).flatMapMany(Flux::fromIterable);
  }

  /**
   * Removes a {@link EventingFunction} by its name if it exists.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to drop.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws EventingFunctionDeployedException (async) if the function is currently deployed (undeploy first).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropFunction(final String name) {
    return dropFunction(name, DropFunctionOptions.dropFunctionOptions());
  }

  /**
   * Removes a {@link EventingFunction} by its name if it exists with custom options.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to drop.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws EventingFunctionDeployedException (async) if the function is currently deployed (undeploy first).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> dropFunction(final String name, final DropFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.dropFunction(name, options));
  }

  /**
   * Deploys an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#DEPLOYED}.
   *
   * @param name the name of the function to deploy.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> deployFunction(final String name) {
    return deployFunction(name, DeployFunctionOptions.deployFunctionOptions());
  }

  /**
   * Deploys an {@link EventingFunction} identified by its name with custom options.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#DEPLOYED}.
   *
   * @param name the name of the function to deploy.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> deployFunction(final String name, final DeployFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.deployFunction(name, options));
  }

  /**
   * Undeploys an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#DEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to undeploy.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> undeployFunction(final String name) {
    return undeployFunction(name, UndeployFunctionOptions.undeployFunctionOptions());
  }

  /**
   * Undeploys an {@link EventingFunction} identified by its name with custom options.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#DEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to undeploy.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> undeployFunction(final String name, final UndeployFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.undeployFunction(name, options));
  }

  /**
   * Pauses an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#RUNNING}
   * to state {@link EventingFunctionProcessingStatus#PAUSED}.
   *
   * @param name the name of the function to pause.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> pauseFunction(final String name) {
    return pauseFunction(name, PauseFunctionOptions.pauseFunctionOptions());
  }

  /**
   * Pauses an {@link EventingFunction} identified by its name with custom options.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#RUNNING}
   * to state {@link EventingFunctionProcessingStatus#PAUSED}.
   *
   * @param name the name of the function to pause.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> pauseFunction(final String name, final PauseFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.pauseFunction(name, options));
  }

  /**
   * Resumes an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#PAUSED}
   * to state {@link EventingFunctionProcessingStatus#RUNNING}.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to resume.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> resumeFunction(final String name) {
    return resumeFunction(name, ResumeFunctionOptions.resumeFunctionOptions());
  }

  /**
   * Resumes an {@link EventingFunction} identified by its name with custom options.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#PAUSED}
   * to state {@link EventingFunctionProcessingStatus#RUNNING}.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to resume.
   * @param options the custom options to apply.
   * @return a {@link Mono} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<Void> resumeFunction(final String name, final ResumeFunctionOptions options) {
    return Reactor.toMono(() -> asyncManager.resumeFunction(name, options));
  }

  /**
   * Retrieves helpful status information about all functions currently created on the cluster.
   *
   * @return a {@link Mono} completing with the eventing status or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<EventingStatus> functionsStatus() {
    return functionsStatus(FunctionsStatusOptions.functionsStatusOptions());
  }

  /**
   * Retrieves helpful status information about all functions currently created on the cluster with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link Mono} completing with the eventing status or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public Mono<EventingStatus> functionsStatus(final FunctionsStatusOptions options) {
    return Reactor.toMono(() -> asyncManager.functionsStatus(options));
  }

}
