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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The compound status an eventing function can be in at any given point in time.
 */
public enum EventingFunctionStatus {
  /**
   * The function is not deployed.
   */
  @JsonProperty("undeployed")
  UNDEPLOYED,
  /**
   * The function is currently being deployed.
   */
  @JsonProperty("deploying")
  DEPLOYING,
  /**
   * The function is deployed.
   */
  @JsonProperty("deployed")
  DEPLOYED,
  /**
   * The function is currently being undeployed.
   */
  @JsonProperty("undeploying")
  UNDEPLOYING,
  /**
   * The function is paused.
   */
  @JsonProperty("paused")
  PAUSED,
  /**
   * The function is currently being paused.
   */
  @JsonProperty("pausing")
  PAUSING;

}
