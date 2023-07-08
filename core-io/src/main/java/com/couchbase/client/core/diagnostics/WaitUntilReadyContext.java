/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.diagnostics.HealthPinger.formatGroupedByNode;
import static java.util.Objects.requireNonNull;

public class WaitUntilReadyContext extends AbstractContext {

  private final Set<ServiceType> serviceTypes;
  private final Duration timeout;
  private final ClusterState desiredState;
  private final Optional<String> bucketName;
  private final Map<ServiceType, List<EndpointDiagnostics>> diagnostics;
  private final WaitUntilReadyHelper.WaitUntilReadyState state;
  private final Set<RequestTarget> remainingPingTargets;

  public WaitUntilReadyContext(
    final Set<ServiceType> serviceTypes,
    final Duration timeout,
    final ClusterState desiredState,
    final Optional<String> bucketName,
    final Map<ServiceType, List<EndpointDiagnostics>> diagnostics,
    final WaitUntilReadyHelper.WaitUntilReadyState state,
    final Set<RequestTarget> remainingPingTargets
  ) {
    this.diagnostics = requireNonNull(diagnostics);
    this.serviceTypes = requireNonNull(serviceTypes);
    this.timeout = requireNonNull(timeout);
    this.desiredState = requireNonNull(desiredState);
    this.bucketName = requireNonNull(bucketName);
    this.state = requireNonNull(state);
    this.remainingPingTargets = requireNonNull(remainingPingTargets);
  }

  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);

    input.put("desiredState", desiredState);
    input.put("checkedServices", serviceTypes);
    input.put("timeoutMs", timeout.toMillis());
    bucketName.ifPresent(b -> input.put("bucket", b));
    input.put("remainingPingTargets", formatGroupedByNode(remainingPingTargets));
    Map<String, Object> services = new LinkedHashMap<>();
    for (Map.Entry<ServiceType, List<EndpointDiagnostics>> service : diagnostics.entrySet()) {
      services.put(service.getKey().ident(), service.getValue().stream().map(EndpointDiagnostics::toMap).collect(Collectors.toList()));
    }
    input.put("services", services);
    input.put("state", state.export());
  }
}
