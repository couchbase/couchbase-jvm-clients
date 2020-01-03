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

package com.couchbase.client.java.diagnostics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;
import java.util.Set;

/**
 * Allows to customize the diagnostics report.
 */
public class DiagnosticsOptions {

  /**
   * Holds a custom report id.
   */
  private Optional<String> reportId = Optional.empty();

  /**
   * Creates a new set of {@link DiagnosticsOptions}.
   *
   * @return options to customize.
   */
  public static DiagnosticsOptions diagnosticsOptions() {
    return new DiagnosticsOptions();
  }

  private DiagnosticsOptions() {}

  /**
   * Sets a custom report ID that will be used in the report.
   * <p>
   * If no report ID is provided, the client will generate a unique one.
   *
   * @param reportId the report ID that should be used for this report.
   * @return the {@link DiagnosticsOptions} to allow method chaining.
   */
  public DiagnosticsOptions reportId(final String reportId) {
    this.reportId = Optional.of(reportId);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built {

    Built() { }
    public Optional<String> reportId() {
      return reportId;
    }

  }

}
