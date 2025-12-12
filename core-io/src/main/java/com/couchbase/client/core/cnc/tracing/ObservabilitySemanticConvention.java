/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.tracing;

import com.couchbase.client.core.annotation.Stability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Spans and metrics can be output in two formats:
 * <p>
 * The first, internally identified as "v0", follows the OpenTelemetry standard prior to its 1.23.0.
 * The second, internally identified as "v1", follows the post-1.23.0 standard.
 * <p>
 * To provide backwards compatibility "v0" is the default if nothing is specified.
 */
@Stability.Uncommitted
public enum ObservabilitySemanticConvention {
  /**
   * Outputs only the post-1.23.0 OpenTelemetry standard ("v1")
   */
  DATABASE,

  /**
   * Outputs both the pre and post-1.23.0 OpenTelemetry standard ("v0" and "v1").
   * This is intended to support users transitioning between the two.
   * Spans will have attributes reflecting both standards.
   * Two versions of each metric will be output.
   */
  DATABASE_DUP;

  private static final Logger LOGGER = LoggerFactory.getLogger(ObservabilitySemanticConvention.class);

  private static final List<String> RECOGNIZED_VALUES = Collections.unmodifiableList(Arrays.stream(ObservabilitySemanticConvention.values())
    .map(v -> v.name().toLowerCase(Locale.ROOT).replace('_', '/'))
    .collect(Collectors.toList()));

  public static List<ObservabilitySemanticConvention> loadAndParseOpenTelemetryEnvVar() {
    String otelEnvVar = System.getProperty("OTEL_SEMCONV_STABILITY_OPT_IN", null);

    if (otelEnvVar == null) {
      return new ArrayList<>();
    }

    List<ObservabilitySemanticConvention> out = Arrays.stream(otelEnvVar.split(","))
      .map(String::trim)
      .filter(s -> {
        if (!RECOGNIZED_VALUES.contains(s)) {
          LOGGER.warn("Did not recognize OTEL_SEMCONV_STABILITY_OPT_IN value '{}', item '{}': error '{}'.  Recognized values are from the set {}, in a comma-separated list.",
            otelEnvVar, s, s, RECOGNIZED_VALUES);
           return false;
        }
        return true;
      })
      .map(String::toUpperCase)
      .map(s -> s.replace('/', '_'))
      .map(s -> {
        try {
          return ObservabilitySemanticConvention.valueOf(s);
        } catch (RuntimeException e) {
          // Should not be possible due to earlier sanity check.
          LOGGER.warn("Did not recognize OTEL_SEMCONV_STABILITY_OPT_IN value '{}' item '{}': error '{}'.  Recognized values are from the set {}, in a comma-separated list.",
            otelEnvVar, s, e.toString(), RECOGNIZED_VALUES);
          return null;
        }
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
    LOGGER.info("Found semantic convention OTEL_SEMCONV_STABILITY_OPT_IN '{}', parsed as: {}", otelEnvVar, out);
    return out;
  }
}
