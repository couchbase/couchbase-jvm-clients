/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.protostellar.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryMetrics;
import com.couchbase.client.core.util.ProtostellarUtil;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.time.Duration;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class ProtostellarCoreQueryMetrics implements CoreQueryMetrics {
  private final QueryResponse.MetaData.Metrics metrics;

  public ProtostellarCoreQueryMetrics(QueryResponse.MetaData.Metrics metrics) {
    this.metrics = notNull(metrics, "metrics");
  }

  @Override
  public Duration elapsedTime() {
    return ProtostellarUtil.convert(metrics.getElapsedTime());
  }

  @Override
  public Duration executionTime() {
    return ProtostellarUtil.convert(metrics.getExecutionTime());
  }

  @Override
  public long sortCount() {
    return metrics.getSortCount();
  }

  @Override
  public long resultCount() {
    return metrics.getResultCount();
  }

  @Override
  public long resultSize() {
    return metrics.getResultSize();
  }

  @Override
  public long mutationCount() {
    return metrics.getMutationCount();
  }

  @Override
  public long errorCount() {
    return metrics.getErrorCount();
  }

  @Override
  public long warningCount() {
    return metrics.getWarningCount();
  }
}
