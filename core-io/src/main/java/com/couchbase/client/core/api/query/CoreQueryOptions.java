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

package com.couchbase.client.core.api.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.transaction.config.CoreSingleQueryTransactionOptions;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@Stability.Internal
public interface CoreQueryOptions {

  boolean adhoc();

  @Nullable
  String clientContextId();

  @Nullable
  CoreMutationState consistentWith();

  @Nullable
  Integer maxParallelism();

  boolean metrics();

  @Nullable
  ObjectNode namedParameters();

  @Nullable
  Integer pipelineBatch();

  @Nullable
  Integer pipelineCap();

  @Nullable
  ArrayNode positionalParameters();

  CoreQueryProfile profile();

  @Nullable
  JsonNode raw();

  boolean readonly();

  @Nullable
  Duration scanWait();

  @Nullable
  Integer scanCap();

  @Nullable
  CoreQueryScanConsistency scanConsistency();

  boolean flexIndex();

  @Nullable
  Boolean preserveExpiry();

  default boolean asTransaction() {
    return asTransactionOptions() != null;
  }

  @Nullable
  CoreSingleQueryTransactionOptions asTransactionOptions();

  CoreCommonOptions commonOptions();
}



