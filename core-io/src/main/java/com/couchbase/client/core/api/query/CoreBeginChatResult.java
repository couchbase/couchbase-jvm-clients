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

import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class CoreBeginChatResult {
  private final String chatId;
  private final String ownerNodeUuid;

  public CoreBeginChatResult(String chatId, String ownerNodeUuid) {
    this.chatId = requireNonNull(chatId);
    this.ownerNodeUuid = requireNonNull(ownerNodeUuid);
  }

  public String chatId() {
    return chatId;
  }

  public String ownerNodeUuid() {
    return ownerNodeUuid;
  }
}
