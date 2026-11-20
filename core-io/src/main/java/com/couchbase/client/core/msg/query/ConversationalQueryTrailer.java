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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.annotation.Stability;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;

@Stability.Internal
public class ConversationalQueryTrailer {

  private final @Nullable String generatedStatement;
  private final byte @Nullable [] requestTokens;
  private final byte @Nullable [] chatTokens;
  private final @Nullable String chatId;

  public ConversationalQueryTrailer(@Nullable String generatedStatement, byte @Nullable [] requestTokens,
                                    byte @Nullable [] chatTokens, @Nullable String chatId) {
    this.generatedStatement = generatedStatement;
    this.requestTokens = requestTokens;
    this.chatTokens = chatTokens;
    this.chatId = chatId;
  }

  public @Nullable String generatedStatement() {
    return generatedStatement;
  }

  public byte @Nullable [] requestTokens() {
    return requestTokens;
  }

  public byte @Nullable [] chatTokens() {
    return chatTokens;
  }

  public @Nullable String chatId() {
    return chatId;
  }

  @Override
  public String toString() {
    return "ConversationalQueryTrailer{" +
      "generatedStatement=" + generatedStatement +
      ", requestTokens=" + (requestTokens == null ? null : new String(requestTokens, StandardCharsets.UTF_8)) +
      ", chatTokens=" + (chatTokens == null ? null : new String(chatTokens, StandardCharsets.UTF_8)) +
      ", chatId=" + chatId +
      '}';
  }

  public static class Builder {
    private @Nullable String generatedStatement;
    private byte @Nullable [] requestTokens;
    private byte @Nullable [] chatTokens;
    private @Nullable String chatId;

    public void generatedStatement(String generatedStatement) {
      this.generatedStatement = generatedStatement;
    }

    public void requestTokens(byte[] requestTokens) {
      this.requestTokens = requestTokens;
    }

    public void chatTokens(byte[] chatTokens) {
      this.chatTokens = chatTokens;
    }

    public void chatId(String chatId) {
      this.chatId = chatId;
    }

    public ConversationalQueryTrailer build() {
      return new ConversationalQueryTrailer(generatedStatement, requestTokens, chatTokens, chatId);
    }
  }
}
