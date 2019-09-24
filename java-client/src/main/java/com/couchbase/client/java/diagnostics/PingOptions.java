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
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.CommonOptions;

import java.util.List;
import java.util.Optional;

@Stability.Volatile
public class PingOptions extends CommonOptions<PingOptions> {

  private Optional<String> reportId = Optional.empty();
  private List<ServiceType> services;

  public static PingOptions pingOptions() {
    return new PingOptions();
  }

  private PingOptions() {}

  public PingOptions reportId(final String reportId) {
    this.reportId = Optional.of(reportId);
    return this;
  }

  public PingOptions services(final List<ServiceType> services) {
    this.services = services;
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

    public List<ServiceType> services() { return services; }
  }
}
