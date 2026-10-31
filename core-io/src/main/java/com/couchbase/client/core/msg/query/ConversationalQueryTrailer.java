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

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ConversationalQueryTrailer {

  private final Optional<String> generatedStatement;
  private final Optional<byte[]> requestTokens;
  private final Optional<byte[]> chatTokens;
  private final Optional<String> chatId;

  public ConversationalQueryTrailer(Optional<String> generatedStatement, Optional<byte[]> requestTokens,
                                    Optional<byte[]> chatTokens, Optional<String> chatId) {
    this.generatedStatement = requireNonNull(generatedStatement);
    this.requestTokens = requireNonNull(requestTokens);
    this.chatTokens = requireNonNull(chatTokens);
    this.chatId = requireNonNull(chatId);
  }

  public Optional<String> generatedStatement() {
    return generatedStatement;
  }

  public Optional<byte[]> requestTokens() {
    return requestTokens;
  }

  public Optional<byte[]> chatTokens() {
    return chatTokens;
  }

  public Optional<String> chatId() {
    return chatId;
  }

  @Override
  public String toString() {
    return "ConversationalQueryTrailer{" +
      "generatedStatement=" + generatedStatement +
      ", requestTokens=" + requestTokens.map(v -> new String(v, StandardCharsets.UTF_8)) +
      ", chatTokens=" + chatTokens.map(v -> new String(v, StandardCharsets.UTF_8)) +
      ", chatId=" + chatId +
      '}';
  }

  public static class Builder {
    private String generatedStatement;
    private byte[] requestTokens;
    private byte[] chatTokens;
    private String chatId;

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
      return new ConversationalQueryTrailer(
          Optional.ofNullable(generatedStatement),
          Optional.ofNullable(requestTokens),
          Optional.ofNullable(chatTokens),
          Optional.ofNullable(chatId));
    }
  }
}
