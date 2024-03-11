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
import com.couchbase.client.java.Cluster;

import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions.getAllFunctionsOptions;

/**
 * Performs management operations on {@link EventingFunction EventingFunctions}.
 */
@Stability.Volatile
public class ScopeEventingFunctionManager {

  /**
   * The underlying async function manager which performs the actual ops and does the conversions.
   */
  private final AsyncScopeEventingFunctionManager asyncManager;

  /**
   * Creates a new {@link ScopeEventingFunctionManager}.
   * <p>
   * This API is not intended to be called by the user directly, use {@link Cluster#eventingFunctions()}
   * instead.
   *
   * @param asyncManager the underlying async manager that performs the ops.
   */
  @Stability.Internal
  public ScopeEventingFunctionManager(AsyncScopeEventingFunctionManager asyncManager) {
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
   * @throws EventingFunctionCompilationFailureException if the function body cannot be compiled.
   * @throws CollectionNotFoundException if the specified collection or scope does not exist.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws EventingFunctionIdenticalKeyspaceException if the source and metadata keyspace are the same.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void upsertFunction(final EventingFunction function) {
    upsertFunction(function, UpsertFunctionOptions.upsertFunctionOptions());
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
   * @throws EventingFunctionCompilationFailureException if the function body cannot be compiled.
   * @throws CollectionNotFoundException if the specified collection or scope does not exist.
   * @throws BucketNotFoundException if the specified bucket does not exist.
   * @throws EventingFunctionIdenticalKeyspaceException if the source and metadata keyspace are the same.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void upsertFunction(final EventingFunction function, final UpsertFunctionOptions options) {
    block(asyncManager.upsertFunction(function, options));
  }

  /**
   * Retrieves a {@link EventingFunction} by its name.
   *
   * @param name the name of the function to retrieve.
   * @return the eventing function found.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public EventingFunction getFunction(final String name) {
    return getFunction(name, GetFunctionOptions.getFunctionOptions());
  }

  /**
   * Retrieves a {@link EventingFunction} by its name with custom options.
   *
   * @param name the name of the function to retrieve.
   * @param options the custom options to apply.
   * @return the eventing function found.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public EventingFunction getFunction(final String name, final GetFunctionOptions options) {
    return block(asyncManager.getFunction(name, options));
  }

  /**
   * Retrieves all {@link EventingFunction EventingFunctions} currently stored on the server.
   * <p>
   * If no functions are found, an empty list is returned.
   *
   * @return eventing functions found or an empty list.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<EventingFunction> getAllFunctions() {
    return getAllFunctions(getAllFunctionsOptions());
  }

  /**
   * Retrieves all {@link EventingFunction EventingFunctions} currently stored on the server with custom options.
   * <p>
   * If no functions are found, an empty list is returned.
   *
   * @param options the custom options to apply.
   * @return eventing functions found or an empty list.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public List<EventingFunction> getAllFunctions(final GetAllFunctionsOptions options) {
    return block(asyncManager.getAllFunctions(options));
  }

  /**
   * Removes a {@link EventingFunction} by its name if it exists.
   * <p>
   * Note that due to a bug on the server, depending on which version is used, both a
   * {@link EventingFunctionNotFoundException} or a {@link EventingFunctionNotDeployedException} can be thrown if
   * a function does not exist.
   *
   * @param name the name of the function to drop.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException if the function is not found on the server (see above).
   * @throws EventingFunctionDeployedException if the function is currently deployed (undeploy first).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropFunction(final String name) {
     dropFunction(name, DropFunctionOptions.dropFunctionOptions());
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
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException if the function is not found on the server (see above).
   * @throws EventingFunctionDeployedException if the function is currently deployed (undeploy first).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void dropFunction(final String name, final DropFunctionOptions options) {
    block(asyncManager.dropFunction(name, options));
  }

  /**
   * Deploys an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#DEPLOYED}.
   *
   * @param name the name of the function to deploy.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void deployFunction(final String name) {
     deployFunction(name, DeployFunctionOptions.deployFunctionOptions());
  }

  /**
   * Deploys an {@link EventingFunction} identified by its name with custom options.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionDeploymentStatus#UNDEPLOYED}
   * to state {@link EventingFunctionDeploymentStatus#DEPLOYED}.
   *
   * @param name the name of the function to deploy.
   * @param options the custom options to apply.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void deployFunction(final String name, final DeployFunctionOptions options) {
     block(asyncManager.deployFunction(name, options));
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
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException if the function is not found on the server (see above).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void undeployFunction(final String name) {
    undeployFunction(name, UndeployFunctionOptions.undeployFunctionOptions());
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
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException if the function is not found on the server (see above).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void undeployFunction(final String name, final UndeployFunctionOptions options) {
    block(asyncManager.undeployFunction(name, options));
  }

  /**
   * Pauses an {@link EventingFunction} identified by its name.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#RUNNING}
   * to state {@link EventingFunctionProcessingStatus#PAUSED}.
   *
   * @param name the name of the function to pause.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void pauseFunction(final String name) {
    pauseFunction(name, PauseFunctionOptions.pauseFunctionOptions());
  }

  /**
   * Pauses an {@link EventingFunction} identified by its name with custom options.
   * <p>
   * Calling this method effectively moves the function from state {@link EventingFunctionProcessingStatus#RUNNING}
   * to state {@link EventingFunctionProcessingStatus#PAUSED}.
   *
   * @param name the name of the function to pause.
   * @param options the custom options to apply.
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotBootstrappedException if the function is not bootstrapped yet (after creating it).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void pauseFunction(final String name, final PauseFunctionOptions options) {
    block(asyncManager.pauseFunction(name, options));
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
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException if the function is not found on the server (see above).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void resumeFunction(final String name) {
    resumeFunction(name, ResumeFunctionOptions.resumeFunctionOptions());
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
   * @throws EventingFunctionNotFoundException if the function is not found on the server.
   * @throws EventingFunctionNotDeployedException if the function is not found on the server (see above).
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public void resumeFunction(final String name, final ResumeFunctionOptions options) {
    block(asyncManager.resumeFunction(name, options));
  }

  /**
   * Retrieves helpful status information about all functions currently created on the cluster.
   *
   * @return the eventing status.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public EventingStatus functionsStatus() {
    return functionsStatus(FunctionsStatusOptions.functionsStatusOptions());
  }

  /**
   * Retrieves helpful status information about all functions currently created on the cluster with custom options.
   *
   * @param options the custom options to apply.
   * @return the eventing status.
   * @throws CouchbaseException if any other generic unhandled/unexpected errors.
   */
  public EventingStatus functionsStatus(final FunctionsStatusOptions options) {
    return block(asyncManager.functionsStatus(options));
  }


}
