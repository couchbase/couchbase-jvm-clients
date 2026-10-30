/*
 * Copyright (c) 2026 Couchbase, Inc.
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
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import reactor.util.annotation.Nullable;

import java.time.Duration;

@Stability.Internal
public interface CoreAskOptions {

  @Nullable
  CoreModelOptions model();

  @Nullable
  CoreModelCredential credential();

  @Nullable
  String hint();

  @Nullable
  Boolean execute();

  @Nullable
  CoreConversationalQueryOutput output();

  @Nullable
  CoreQueryScanConsistency scanConsistency();

  @Nullable
  CoreMutationState consistentWith();

  @Nullable
  Duration scanWait();

  @Nullable
  Boolean useReplica();

  @Nullable
  String clientContextId();

  @Nullable
  Boolean metrics();

  @Nullable
  CoreQueryProfile profile();

  @Nullable
  JsonNode raw();

  CoreCommonOptions commonOptions();
}
