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

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public final class CoreConversationalQueryMetaData {
  private final CoreQueryMetaData base;
  private final Optional<CoreTokenUsage> requestTokens;
  private final Optional<CoreTokenUsage> chatTokens;
  private final Optional<Duration> naturalLanguageProcessingTime;
  private final Optional<Boolean> generatedStatementExecuted;

  public CoreConversationalQueryMetaData(CoreQueryMetaData base,
                                         Optional<CoreTokenUsage> requestTokens,
                                         Optional<CoreTokenUsage> chatTokens,
                                         Optional<Duration> naturalLanguageProcessingTime,
                                         Optional<Boolean> generatedStatementExecuted) {
    this.base = requireNonNull(base);
    this.requestTokens = requireNonNull(requestTokens);
    this.chatTokens = requireNonNull(chatTokens);
    this.naturalLanguageProcessingTime = requireNonNull(naturalLanguageProcessingTime);
    this.generatedStatementExecuted = requireNonNull(generatedStatementExecuted);
  }

  public String requestId() {
    return base.requestId();
  }

  public String clientContextId() {
    return base.clientContextId();
  }

  public CoreQueryStatus status() {
    return base.status();
  }

  public List<CoreQueryWarning> warnings() {
    return base.warnings();
  }

  public Optional<CoreQueryMetrics> metrics() {
    return base.metrics();
  }

  public Optional<byte[]> profile() {
    return base.profile();
  }

  public Optional<CoreTokenUsage> requestTokens() {
    return requestTokens;
  }

  public Optional<CoreTokenUsage> chatTokens() {
    return chatTokens;
  }

  public Optional<Duration> naturalLanguageProcessingTime() {
    return naturalLanguageProcessingTime;
  }

  public Optional<Boolean> generatedStatementExecuted() {
    return generatedStatementExecuted;
  }

  @Override
  public String toString() {
    return "CoreConversationalQueryMetaData{" +
        "base=" + base +
        ", requestTokens=" + requestTokens +
        ", chatTokens=" + chatTokens +
        ", naturalLanguageProcessingTime=" + naturalLanguageProcessingTime +
        ", generatedStatementExecuted=" + generatedStatementExecuted +
        '}';
  }
}
