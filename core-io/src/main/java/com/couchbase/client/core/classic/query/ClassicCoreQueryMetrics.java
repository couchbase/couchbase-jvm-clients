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

package com.couchbase.client.core.classic.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.query.CoreQueryMetrics;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.Golang;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static com.couchbase.client.core.util.Validators.notNull;

@Stability.Internal
public class ClassicCoreQueryMetrics implements CoreQueryMetrics {
  private final Duration elapsedTime;
  private final Duration executionTime;
  private final long sortCount;
  private final long resultCount;
  private final long resultSize;
  private final long mutationCount;
  private final long errorCount;
  private final long warningCount;

  public ClassicCoreQueryMetrics(final byte[] raw) {
    notNull(raw, "raw");
    try {
      JsonNode rootNode = Mapper.reader().readTree(raw);
      this.elapsedTime = decode(rootNode, String.class, "elapsedTime").map(Golang::parseDuration).orElse(Duration.ZERO);
      this.executionTime = decode(rootNode, String.class, "executionTime").map(Golang::parseDuration).orElse(Duration.ZERO);
      this.sortCount = decode(rootNode, Long.class, "sortCount").orElse(0L);
      this.resultCount = decode(rootNode, Long.class, "resultCount").orElse(0L);
      this.resultSize = decode(rootNode, Long.class, "resultSize").orElse(0L);
      this.mutationCount = decode(rootNode, Long.class, "mutationCount").orElse(0L);
      this.errorCount = decode(rootNode, Long.class, "errorCount").orElse(0L);
      this.warningCount = decode(rootNode, Long.class, "warningCount").orElse(0L);
    } catch (IOException e) {
      throw new DecodingFailureException("Could not decode query metrics");
    }
  }

  public Duration elapsedTime() {
    return elapsedTime;
  }

  public Duration executionTime() {
    return executionTime;
  }

  public long sortCount() {
    return sortCount;
  }

  public long resultCount() {
    return resultCount;
  }

  public long resultSize() {
    return resultSize;
  }

  public long mutationCount() {
    return mutationCount;
  }

  public long errorCount() {
    return errorCount;
  }

  public long warningCount() {
    return warningCount;
  }

  /**
   * Helper method to turn a given path of the raw data into the target class.
   *
   * @param target the target class to decode into.
   * @param path the path of the raw json.
   * @param <T> the generic type to decide into.
   * @return the generic decoded object if present and not null.
   */
  private <T> Optional<T> decode(JsonNode rootNode, Class<T> target, String path) {
    try {
      JsonNode subNode = rootNode.path(path);
      if (subNode == null || subNode.isNull() || subNode.isMissingNode()) {
        return Optional.empty();
      }
      return Optional.ofNullable(Mapper.reader().treeToValue(subNode, target));
    } catch (JsonProcessingException e) {
      throw new DecodingFailureException("Could not decode " + path + " in query metrics!");
    }
  }
}
