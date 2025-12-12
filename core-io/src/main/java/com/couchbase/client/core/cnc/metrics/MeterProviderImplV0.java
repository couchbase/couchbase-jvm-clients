/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.metrics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.TracingDecoratorImpl;
import com.couchbase.client.core.cnc.tracing.TracingDecoratorImplV0;
import com.couchbase.client.core.topology.ClusterIdentifier;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Provides the original V0 implementation of metrics.
 */
@Stability.Internal
public class MeterProviderImplV0 implements MeterProviderImpl {
  private final static TracingDecoratorImpl tip = new TracingDecoratorImplV0();
  private final Meter originalMeter;

  public MeterProviderImplV0(Meter originalMeter) {
    this.originalMeter = requireNonNull(originalMeter);
  }

  // Note most of the comments in MeterProviderImplV1 also apply here, but are not C&Ped for DRY.
  private Map<String, String> tags(ResponseMetricIdentifier rmi) {
    Map<String, String> tags = new HashMap<>(9);
    tags.put(tip.requireAttributeName(TracingAttribute.SYSTEM), TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    tags.put(tip.requireAttributeName(TracingAttribute.SERVICE), rmi.service());
    tags.put(tip.requireAttributeName(TracingAttribute.OPERATION), rmi.operation());

    if (!rmi.isDefaultLoggingMeter()) {
      tags.put(tip.requireAttributeName(TracingAttribute.BUCKET_NAME), rmi.bucketName());
      tags.put(tip.requireAttributeName(TracingAttribute.SCOPE_NAME), rmi.scopeName());
      tags.put(tip.requireAttributeName(TracingAttribute.COLLECTION_NAME), rmi.collectionName());

      ClusterIdentifier clusterIdent = rmi.clusterIdent();
      tags.put(tip.requireAttributeName(TracingAttribute.CLUSTER_UUID), clusterIdent == null ? null : clusterIdent.clusterUuid());
      tags.put(tip.requireAttributeName(TracingAttribute.CLUSTER_NAME), clusterIdent == null ? null : clusterIdent.clusterName());

      if (rmi.exceptionSimpleName() != null) {
        tags.put(tip.requireAttributeName(TracingAttribute.OUTCOME), rmi.exceptionSimpleName());
      } else {
        tags.put(tip.requireAttributeName(TracingAttribute.OUTCOME), "Success");
      }
    }

    return tags;
  }

  private String mapName(CounterName metric) {
    switch (metric) {
      case TRANSACTIONS_COUNTER:
        return "db.couchbase.transactions.total";
      case TRANSACTIONS_ATTEMPT_COUNTER:
        return "db.couchbase.transactions.attempts";
      default:
        throw new IllegalArgumentException("Unknown metric " + metric);
    }
  }

  private String mapName(ValueRecorderName metric) {
    switch (metric) {
      case OPERATIONS:
        return tip.meterOperations();
      default:
        throw new IllegalArgumentException("Unknown metric " + metric);
    }
  }

  @Override
  public Counter counter(CounterName metric, ResponseMetricIdentifier rmi) {
    return originalMeter.counter(mapName(metric), tags(rmi));
  }

  @Override
  public ValueRecorder valueRecorder(ValueRecorderName metric, ResponseMetricIdentifier rmi) {
    return originalMeter.valueRecorder(mapName(metric), tags(rmi));
  }
}
