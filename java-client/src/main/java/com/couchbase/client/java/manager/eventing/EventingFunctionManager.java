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

import com.couchbase.client.core.annotation.Stability;

import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions.getAllFunctionsOptions;

@Stability.Uncommitted
public class EventingFunctionManager {

  private final AsyncEventingFunctionManager asyncManager;

  public EventingFunctionManager(AsyncEventingFunctionManager asyncManager) {
    this.asyncManager = asyncManager;
  }

  public void upsertFunction(final EventingFunction function) {
    upsertFunction(function, UpsertFunctionOptions.upsertFunctionOptions());
  }

  public void upsertFunction(final EventingFunction function, final UpsertFunctionOptions options) {
    block(asyncManager.upsertFunction(function, options));
  }

  public EventingFunction getFunction(final String name) {
    return getFunction(name, GetFunctionOptions.getFunctionOptions());
  }

  public EventingFunction getFunction(final String name, final GetFunctionOptions options) {
    return block(asyncManager.getFunction(name, options));
  }

  public void dropFunction(final String name) {
     dropFunction(name, DropFunctionOptions.dropFunctionOptions());
  }

  public void dropFunction(final String name, final DropFunctionOptions options) {
    block(asyncManager.dropFunction(name, options));
  }

  public void deployFunction(final String name) {
     deployFunction(name, DeployFunctionOptions.deployFunctionOptions());
  }

  public void deployFunction(final String name, final DeployFunctionOptions options) {
     block(asyncManager.deployFunction(name, options));
  }

  public List<EventingFunction> getAllFunctions() {
    return getAllFunctions(getAllFunctionsOptions());
  }

  public List<EventingFunction> getAllFunctions(final GetAllFunctionsOptions options) {
    return block(asyncManager.getAllFunctions(options));
  }

  public void pauseFunction(final String name) {
    pauseFunction(name, PauseFunctionOptions.pauseFunctionOptions());
  }

  public void pauseFunction(final String name, final PauseFunctionOptions options) {
    block(asyncManager.pauseFunction(name, options));
  }

  public void resumeFunction(final String name) {
    resumeFunction(name, ResumeFunctionOptions.resumeFunctionOptions());
  }

  public void resumeFunction(final String name, final ResumeFunctionOptions options) {
    block(asyncManager.resumeFunction(name, options));
  }

  public void undeployFunction(final String name) {
    undeployFunction(name, UndeployFunctionOptions.undeployFunctionOptions());
  }

  public void undeployFunction(final String name, final UndeployFunctionOptions options) {
    block(asyncManager.undeployFunction(name, options));
  }

  public EventingStatus functionsStatus() {
    return  functionsStatus(FunctionsStatusOptions.functionsStatusOptions());
  }

  public EventingStatus functionsStatus(final FunctionsStatusOptions options) {
    return block(asyncManager.functionsStatus(options));
  }


}
