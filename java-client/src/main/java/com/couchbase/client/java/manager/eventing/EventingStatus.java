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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Contains the status of all eventing functions stored on the server.
 */
@Stability.Uncommitted
@JsonIgnoreProperties(ignoreUnknown = true)
public class EventingStatus {

  private final long numEventingNodes;
  private final List<EventingFunctionState> functions;

  @JsonCreator
  @Stability.Internal
  EventingStatus(
    @JsonProperty("num_eventing_nodes") long numEventingNodes,
    @JsonProperty("apps") List<EventingFunctionState> functions
  ) {
    this.numEventingNodes = numEventingNodes;
    this.functions = functions;
  }

  /**
   * Returns the number of eventing nodes participating.
   *
   * @return the number of eventing nodes.
   */
  public long numEventingNodes() {
    return numEventingNodes;
  }

  /**
   * State information for each individual eventing function.
   *
   * @return a (potentially empty) list of eventing function states.
   */
  public List<EventingFunctionState> functions() {
    return functions;
  }

  @Override
  public String toString() {
    return "EventingStatus{" +
      "numEventingNodes=" + numEventingNodes +
      ", functions=" + functions +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EventingStatus that = (EventingStatus) o;
    return numEventingNodes == that.numEventingNodes && Objects.equals(functions, that.functions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(numEventingNodes, functions);
  }

}

