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
import com.couchbase.client.core.api.query.CoreQueryMetaData;
import com.couchbase.client.core.api.query.CoreQueryMetrics;
import com.couchbase.client.core.api.query.CoreQueryStatus;
import com.couchbase.client.core.api.query.CoreQueryWarning;
import com.couchbase.client.core.error.ErrorCodeAndMessage;
import com.couchbase.client.protostellar.query.v1.QueryResponse;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class ProtostellarCoreQueryMetaData extends CoreQueryMetaData {
  private final QueryResponse.MetaData metaData;

  public ProtostellarCoreQueryMetaData(QueryResponse.MetaData metaData) {
    this.metaData = notNull(metaData, "metaData");
  }

  @Override
  public String requestId() {
    return metaData.getRequestId();
  }

  @Override
  public String clientContextId() {
    return metaData.getClientContextId();
  }

  @Override
  public CoreQueryStatus status() {
    switch (metaData.getStatus()) {
      case RUNNING:
        return CoreQueryStatus.RUNNING;
      case SUCCESS:
        return CoreQueryStatus.SUCCESS;
      case ERRORS:
        return CoreQueryStatus.ERRORS;
      case COMPLETED:
        return CoreQueryStatus.COMPLETED;
      case STOPPED:
        return CoreQueryStatus.STOPPED;
      case TIMEOUT:
        return CoreQueryStatus.TIMEOUT;
      case CLOSED:
        return CoreQueryStatus.CLOSED;
      case FATAL:
        return CoreQueryStatus.FATAL;
      case ABORTED:
        return CoreQueryStatus.ABORTED;
    }
    return CoreQueryStatus.UNKNOWN;
  }

  @Override
  public Optional<byte[]> signature() {
    return Optional.of(metaData.getSignature().toByteArray());
  }

  @Override
  public Optional<byte[]> profile() {
    if (metaData.hasProfile()) {
      return Optional.of(metaData.getProfile().toByteArray());
    }
    return Optional.empty();
  }

  @Override
  public Optional<CoreQueryMetrics> metrics() {
    if (metaData.hasMetrics()) {
      return Optional.of(new ProtostellarCoreQueryMetrics(metaData.getMetrics()));
    }
    return Optional.empty();
  }

  @Override
  public List<CoreQueryWarning> warnings() {
    return metaData.getWarningsList()
        .stream()
        .map(warning -> new CoreQueryWarning(new ErrorCodeAndMessage(warning.getCode(), warning.getMessage(), false, null)))
        .collect(Collectors.toList());
  }
}
