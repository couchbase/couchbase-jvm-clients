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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.EventingFunctionCompilationFailureException;
import com.couchbase.client.core.error.EventingFunctionDeployedException;
import com.couchbase.client.core.error.EventingFunctionIdenticalKeyspaceException;
import com.couchbase.client.core.error.EventingFunctionNotBootstrappedException;
import com.couchbase.client.core.error.EventingFunctionNotDeployedException;
import com.couchbase.client.core.error.EventingFunctionNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreEventingFunctionManager;
import com.couchbase.client.core.util.PreventsGarbageCollection;
import com.couchbase.client.java.AsyncCluster;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.manager.eventing.AsyncEventingFunctionManager.encodeFunction;
import static com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions.getAllFunctionsOptions;
import static java.util.Objects.requireNonNull;

/**
 * Performs management operations on {@link EventingFunction EventingFunctions}.
 */
@Stability.Volatile
public class AsyncScopeEventingFunctionManager {

  /**
   * References the core-io eventing function manager which abstracts common I/O functionality.
   */
  private final CoreEventingFunctionManager coreManager;

  @PreventsGarbageCollection
  private final AsyncCluster cluster;

  /**
   * Creates a new {@link AsyncScopeEventingFunctionManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link AsyncCluster#eventingFunctions()}
   * instead.
   *
   * @param core the internal core reference.
   */
  @Stability.Internal
  public AsyncScopeEventingFunctionManager(
    final Core core,
    final AsyncCluster cluster,
    @Nullable final CoreBucketAndScope scope
  ) {
    this.coreManager = new CoreEventingFunctionManager(core, scope);
    this.cluster = requireNonNull(cluster);
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionCompilationFailureException (async) if the function body cannot be compiled.
   * @throws CollectionNotFoundException (async) if the specified collection or scope does not exist.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws EventingFunctionIdenticalKeyspaceException (async) if the source and metadata keyspace are the same.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> upsertFunction(final EventingFunction function) {
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionCompilationFailureException (async) if the function body cannot be compiled.
   * @throws CollectionNotFoundException (async) if the specified collection or scope does not exist.
   * @throws BucketNotFoundException (async) if the specified bucket does not exist.
   * @throws EventingFunctionIdenticalKeyspaceException (async) if the source and metadata keyspace are the same.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> upsertFunction(final EventingFunction function, final UpsertFunctionOptions options) {
    return coreManager.upsertFunction(function.name(), encodeFunction(function), options.build());
  }

  /**
   * Retrieves a {@link EventingFunction} by its name.
   *
   * @param name the name of the function to retrieve.
   * @return a {@link CompletableFuture} completing with the eventing function found or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<EventingFunction> getFunction(final String name) {
    return getFunction(name, GetFunctionOptions.getFunctionOptions());
  }

  /**
   * Retrieves a {@link EventingFunction} by its name with custom options.
   *
   * @param name the name of the function to retrieve.
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with the eventing function found or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<EventingFunction> getFunction(final String name, final GetFunctionOptions options) {
    return coreManager
      .getFunction(name, options.build())
      .thenApply(AsyncEventingFunctionManager::decodeFunction);
  }

  /**
   * Retrieves all {@link EventingFunction EventingFunctions} currently stored on the server.
   * <p>
   * If no functions are found, an empty list is returned.
   *
   * @return a {@link CompletableFuture} completing with all eventing functions found or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<EventingFunction>> getAllFunctions() {
    return getAllFunctions(getAllFunctionsOptions());
  }

  /**
   * Retrieves all {@link EventingFunction EventingFunctions} currently stored on the server with custom options.
   * <p>
   * If no functions are found, an empty list is returned.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with all eventing functions found or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<List<EventingFunction>> getAllFunctions(final GetAllFunctionsOptions options) {
    return coreManager
      .getAllFunctions(options.build())
      .thenApply(AsyncEventingFunctionManager::decodeFunctions);
  }

  /**
   * Removes a {@link EventingFunction} by its name if it exists.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to drop.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws EventingFunctionDeployedException (async) if the function is currently deployed (undeploy first).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropFunction(final String name) {
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws EventingFunctionDeployedException (async) if the function is currently deployed (undeploy first).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> dropFunction(final String name, final DropFunctionOptions options) {
    return coreManager.dropFunction(name, options.build());
  }

  /**
   * Deploys an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#DEPLOYED}.
   *
   * @param name the name of the function to deploy.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> deployFunction(final String name) {
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> deployFunction(final String name, final DeployFunctionOptions options) {
    return coreManager.deployFunction(name, options.build());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> undeployFunction(final String name) {
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> undeployFunction(final String name, final UndeployFunctionOptions options) {
    return coreManager.undeployFunction(name, options.build());
  }

  /**
   * Pauses an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#RUNNING}
   * to state {@link EventingFunctionProcessingStatus#PAUSED}.
   *
   * @param name the name of the function to pause.
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> pauseFunction(final String name) {
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException (async) if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> pauseFunction(final String name, final PauseFunctionOptions options) {
    return coreManager.pauseFunction(name, options.build());
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> resumeFunction(final String name) {
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
   * @return a {@link CompletableFuture} completing when the operation is applied or failed with an error.
   * @throws EventingFunctionNotFoundException (async) if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException (async) if the function is not found on the server (see above).
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<Void> resumeFunction(final String name, final ResumeFunctionOptions options) {
    return coreManager.resumeFunction(name, options.build());
  }

  /**
   * Retrieves helpful status information about all functions currently created on the cluster.
   *
   * @return a {@link CompletableFuture} completing with the eventing status or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<EventingStatus> functionsStatus() {
    return functionsStatus(FunctionsStatusOptions.functionsStatusOptions());
  }

  /**
   * Retrieves helpful status information about all functions currently created on the cluster with custom options.
   *
   * @param options the custom options to apply.
   * @return a {@link CompletableFuture} completing with the eventing status or failed with an error.
   * @throws CouchbaseException (async) if any other generic unhandled/unexpected errors.
   */
  public CompletableFuture<EventingStatus> functionsStatus(final FunctionsStatusOptions options) {
    return coreManager
      .functionsStatus(options.build())
      .thenApply(bytes -> Mapper.decodeInto(bytes, EventingStatus.class));
  }
}
