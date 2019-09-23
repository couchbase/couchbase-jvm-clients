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
import com.couchbase.client.java.CommonOptions;

import java.util.Optional;

public class DiagnosticsOptions extends CommonOptions<DiagnosticsOptions> {

  private Optional<String> reportId = Optional.empty();

  public static DiagnosticsOptions diagnosticsOptions() {
    return new DiagnosticsOptions();
  }

  private DiagnosticsOptions() {}

  public DiagnosticsOptions reportId(final String reportId) {
    this.reportId = Optional.of(reportId);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    public Optional<String> reportId() {
      return reportId;
    }
  }
}
