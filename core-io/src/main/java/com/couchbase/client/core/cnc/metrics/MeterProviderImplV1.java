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
import com.couchbase.client.core.cnc.ValueRecorder;
import com.couchbase.client.core.cnc.tracing.TracingAttribute;
import com.couchbase.client.core.cnc.tracing.TracingDecoratorImpl;
import com.couchbase.client.core.cnc.tracing.TracingDecoratorImplV1;
import com.couchbase.client.core.topology.ClusterIdentifier;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Provides the V1 implementation of metrics, updated to follow revised OpenTelemetry standard.
 */
@Stability.Internal
public class MeterProviderImplV1 implements MeterProviderImpl {
  private final static TracingDecoratorImpl tip = new TracingDecoratorImplV1();
  private final Meter originalMeter;

  public MeterProviderImplV1(Meter originalMeter) {
    this.originalMeter = requireNonNull(originalMeter);
  }

  private Map<String, String> tags(ResponseMetricIdentifier rmi) {
    // N.b. remember the pattern is that ResponseMetricIdentifier is created many times (on every request),
    // while this method will only be called if we don't already have a corresponding ResponseMetricIdentifier
    // entry in the map.  E.g. it would be a false optimisation to move this tags() logic into the ctor.
    Map<String, String> tags = new HashMap<>(10);
    tags.put(tip.requireAttributeName(TracingAttribute.SERVICE), rmi.service());
    tags.put(tip.requireAttributeName(TracingAttribute.OPERATION), rmi.operation());
    // V1 addition
    tags.put(MeterConventions.METRIC_TAG_UNITS, MeterConventions.METRIC_TAG_UNIT_SECONDS);

    // The LoggingMeter only uses the service and operation labels, so optimise this hot-path by skipping
    // assigning other labels.
    if (!rmi.isDefaultLoggingMeter()) {
      // Crucial note for Micrometer:
      // If we are ever going to output an attribute from a given JVM run then we must always
      // output that attribute in this run.  Specifying null as an attribute value allows the OTel backend to strip it, and
      // the Micrometer backend to provide a default value.
      // See (internal to Couchbase) discussion here for full details:
      // https://issues.couchbase.com/browse/CBSE-17070?focusedId=779820&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-779820
      // If this rule is not followed, then Micrometer will silently discard some metrics.  Micrometer requires that
      // every value output under a given metric has the same set of attributes.

      tags.put(tip.requireAttributeName(TracingAttribute.BUCKET_NAME), rmi.bucketName());
      tags.put(tip.requireAttributeName(TracingAttribute.SCOPE_NAME), rmi.scopeName());
      tags.put(tip.requireAttributeName(TracingAttribute.COLLECTION_NAME), rmi.collectionName());

      ClusterIdentifier clusterIdent = rmi.clusterIdent();
      tags.put(tip.requireAttributeName(TracingAttribute.CLUSTER_UUID), clusterIdent == null ? null : clusterIdent.clusterUuid());
      tags.put(tip.requireAttributeName(TracingAttribute.CLUSTER_NAME), clusterIdent == null ? null : clusterIdent.clusterName());

      if (rmi.exceptionSimpleName() != null) {
        tags.put(tip.requireAttributeName(TracingAttribute.OUTCOME), rmi.exceptionSimpleName());
      }
      // "Success" not output in V1
    }

    return tags;
  }

  private String mapName(CounterName metric) {
    switch (metric) {
      case TRANSACTIONS_COUNTER:
        return "couchbase.client.transactions.total";
      case TRANSACTIONS_ATTEMPT_COUNTER:
        return "couchbase.client.transactions.attempts";
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
