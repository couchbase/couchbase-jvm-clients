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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EventingFunctionState {

  private final String name;
  private final EventingFunctionStatus status;
  private final long numBootstrappingNodes;
  private final long numDeployedNodes;
  private final EventingFunctionDeploymentStatus deploymentStatus;
  private final EventingFunctionProcessingStatus processingStatus;

  @JsonCreator
  EventingFunctionState(
    @JsonProperty("name") String name,
    @JsonProperty("composite_status") EventingFunctionStatus status,
    @JsonProperty("num_bootstrapping_nodes") long numBootstrappingNodes,
    @JsonProperty("num_deployed_nodes") long numDeployedNodes,
    @JsonProperty("deployment_status") boolean deploymentStatus,
    @JsonProperty("processing_status") boolean processingStatus
  ) {
    this.name = name;
    this.status = status;
    this.numBootstrappingNodes = numBootstrappingNodes;
    this.numDeployedNodes = numDeployedNodes;
    this.deploymentStatus = deploymentStatus
      ? EventingFunctionDeploymentStatus.DEPLOYED
      : EventingFunctionDeploymentStatus.UNDEPLOYED;
    this.processingStatus = processingStatus
      ? EventingFunctionProcessingStatus.RUNNING
      : EventingFunctionProcessingStatus.PAUSED;
  }

  /**
   * The name of the eventing function.
   */
  public String name() {
    return name;
  }

  /**
   * The status the function is currently in (including transitional states).
   */
  public EventingFunctionStatus status() {
    return status;
  }

  /**
   * The number of nodes which are currently bootstrapping.
   */
  public long numBootstrappingNodes() {
    return numBootstrappingNodes;
  }

  /**
   * The number of deployed nodes.
   */
  public long numDeployedNodes() {
    return numDeployedNodes;
  }

  /**
   * The current deployment status.
   */
  public EventingFunctionDeploymentStatus deploymentStatus() {
    return deploymentStatus;
  }

  /**
   * The current processing status.
   */
  public EventingFunctionProcessingStatus processingStatus() {
    return processingStatus;
  }

  @Override
  public String toString() {
    return "EventingFunctionState{" +
      "name='" + name + '\'' +
      ", status=" + status +
      ", numBootstrappingNodes=" + numBootstrappingNodes +
      ", numDeployedNodes=" + numDeployedNodes +
      ", deploymentStatus=" + deploymentStatus +
      ", processingStatus=" + processingStatus +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventingFunctionState that = (EventingFunctionState) o;
    return numBootstrappingNodes == that.numBootstrappingNodes && numDeployedNodes == that.numDeployedNodes && Objects.equals(name, that.name) && status == that.status && deploymentStatus == that.deploymentStatus && processingStatus == that.processingStatus;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, status, numBootstrappingNodes, numDeployedNodes, deploymentStatus, processingStatus);
  }
}
