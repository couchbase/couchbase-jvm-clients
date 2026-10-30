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

package com.couchbase.client.core.protostellar.query;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.api.query.CoreAskOptions;
import com.couchbase.client.core.api.query.CoreBeginChatOptions;
import com.couchbase.client.core.api.query.CoreBeginChatResult;
import com.couchbase.client.core.api.query.CoreChatState;
import com.couchbase.client.core.api.query.CoreConversationalQueryMetaData;
import com.couchbase.client.core.api.query.CoreConversationalQueryOps;
import com.couchbase.client.core.api.query.CoreConversationalQueryOptions;
import com.couchbase.client.core.api.query.CoreConversationalQueryResult;
import com.couchbase.client.core.api.query.CorePauseOptions;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.error.FeatureNotAvailableException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * See comment in CoreProtostellar on why this class exists.
 */
@Stability.Internal
public class ProtostellarCoreConversationalQueryOps implements CoreConversationalQueryOps {

  @Override
  public CoreAsyncResponse<CoreConversationalQueryResult> queryAsync(String prompt,
                                                                    List<CoreKeyspace> keyspaces,
                                                                    CoreConversationalQueryOptions options) {
    return unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreBeginChatResult> beginChatAsync(List<CoreKeyspace> keyspaces, CoreBeginChatOptions options) {
    return unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryResult> askAsync(CoreChatState chat, String prompt, CoreAskOptions options) {
    return unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryMetaData> pauseAsync(CoreChatState chat, CorePauseOptions options) {
    return unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryMetaData> resumeAsync(CoreChatState chat, CoreCommonOptions options) {
    return unsupported();
  }

  @Override
  public CoreAsyncResponse<CoreConversationalQueryMetaData> endAsync(CoreChatState chat, CoreCommonOptions options) {
    return unsupported();
  }

  private static <T> CoreAsyncResponse<T> unsupported() {
    CompletableFuture<T> failed = new CompletableFuture<>();
    failed.completeExceptionally(new FeatureNotAvailableException("Conversational query cannot be used together with couchbase2://"));
    return new CoreAsyncResponse<>(failed, () -> {
    });
  }
}
