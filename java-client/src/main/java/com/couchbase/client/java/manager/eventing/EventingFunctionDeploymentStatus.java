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

/**
 * The deployment status of the eventing function.
 */
public enum EventingFunctionDeploymentStatus {
  /**
   * The function is currently deployed.
   */
  DEPLOYED,
  /**
   * The function is currently undeployed.
   */
  UNDEPLOYED;

  /**
   * Returns true if the function is currently deployed.
   */
  public boolean isDeployed() {
    return this == DEPLOYED;
  }
}
