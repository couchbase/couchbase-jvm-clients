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

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreAsyncResponse;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;

import java.util.List;

@Stability.Internal
public interface CoreConversationalQueryOps {

  default CoreConversationalQueryResult queryBlocking(String prompt,
                                                      List<CoreKeyspace> keyspaces,
                                                      CoreConversationalQueryOptions options) {
    return queryAsync(prompt, keyspaces, options).toBlocking();
  }

  CoreAsyncResponse<CoreConversationalQueryResult> queryAsync(String prompt,
                                                              List<CoreKeyspace> keyspaces,
                                                              CoreConversationalQueryOptions options);

  default CoreBeginChatResult beginChatBlocking(List<CoreKeyspace> keyspaces, CoreBeginChatOptions options) {
    return beginChatAsync(keyspaces, options).toBlocking();
  }

  CoreAsyncResponse<CoreBeginChatResult> beginChatAsync(List<CoreKeyspace> keyspaces, CoreBeginChatOptions options);

  default CoreConversationalQueryResult askBlocking(CoreChatState chat, String prompt, CoreAskOptions options) {
    return askAsync(chat, prompt, options).toBlocking();
  }

  CoreAsyncResponse<CoreConversationalQueryResult> askAsync(CoreChatState chat, String prompt, CoreAskOptions options);

  default CoreConversationalQueryMetaData pauseBlocking(CoreChatState chat, CorePauseOptions options) {
    return pauseAsync(chat, options).toBlocking();
  }

  CoreAsyncResponse<CoreConversationalQueryMetaData> pauseAsync(CoreChatState chat, CorePauseOptions options);

  default CoreConversationalQueryMetaData resumeBlocking(CoreChatState chat, CoreCommonOptions options) {
    return resumeAsync(chat, options).toBlocking();
  }

  CoreAsyncResponse<CoreConversationalQueryMetaData> resumeAsync(CoreChatState chat, CoreCommonOptions options);

  default CoreConversationalQueryMetaData endBlocking(CoreChatState chat, CoreCommonOptions options) {
    return endAsync(chat, options).toBlocking();
  }

  CoreAsyncResponse<CoreConversationalQueryMetaData> endAsync(CoreChatState chat, CoreCommonOptions options);
}
