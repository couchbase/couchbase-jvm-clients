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
import reactor.util.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * The client-side state of a chat: its id, and the routing information that the chat operations maintain.
 */
@Stability.Internal
public final class CoreChatState {
  private final String chatId;
  // The nodeUUID of the query node that chat operations are routed to, if one is retained.
  private volatile @Nullable String owner;
  // Set after a successful END.
  private volatile boolean closed;

  public CoreChatState(String chatId, @Nullable String owner) {
    this.chatId = requireNonNull(chatId);
    this.owner = owner;
  }

  public String chatId() {
    return chatId;
  }

  public @Nullable String owner() {
    return owner;
  }

  public void owner(@Nullable String owner) {
    this.owner = owner;
  }

  public boolean closed() {
    return closed;
  }

  public void close() {
    this.closed = true;
  }

  @Override
  public String toString() {
    return "CoreChatState{" +
        "chatId='" + chatId + '\'' +
        ", owner='" + owner + '\'' +
        ", closed=" + closed +
        '}';
  }
}
