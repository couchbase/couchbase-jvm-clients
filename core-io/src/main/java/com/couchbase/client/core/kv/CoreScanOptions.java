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

package com.couchbase.client.core.kv;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.shared.CoreMutationState;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.util.annotation.Nullable;

/**
 * Allows to customize the various range and sampling scan options.
 */
@Stability.Internal
public interface CoreScanOptions {

  CoreCommonOptions commonOptions();

  boolean idsOnly();

  @Nullable
  CoreRangeScanSort sort();

  @Nullable
  CoreMutationState consistentWith();

  int batchItemLimit();

  int batchByteLimit();

  default Map<Short, MutationToken> consistencyMap(){
    return consistentWith() == null ? new HashMap<>() : consistentWith().toMap();
  }
}
