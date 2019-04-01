/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.msg.analytics;

import com.couchbase.client.core.msg.chunk.ChunkTrailer;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class AnalyticsChunkTrailer implements ChunkTrailer {

  private final String status;
  private final byte[] metrics;
  private final Optional<byte[]> warnings;
  private final Optional<byte[]> errors;

  public AnalyticsChunkTrailer(String status, byte[] metrics, Optional<byte[]> warnings,
                               Optional<byte[]> errors) {
    this.status = status;
    this.metrics = metrics;
    this.warnings = warnings;
    this.errors = errors;
  }

  public String status() {
    return status;
  }

  public byte[] metrics() {
    return metrics;
  }

  public Optional<byte[]> warnings() {
    return warnings;
  }

  public Optional<byte[]> errors() {
    return errors;
  }

  @Override
  public String toString() {
    return "AnalyticsChunkTrailer{" +
      "status='" + status + '\'' +
      ", metrics=" + new String(metrics, StandardCharsets.UTF_8) +
      ", warnings=" + warnings.map(v -> new String(v, StandardCharsets.UTF_8)) +
      ", errors=" + errors.map(v -> new String(v, StandardCharsets.UTF_8)) +
      '}';
  }
}
