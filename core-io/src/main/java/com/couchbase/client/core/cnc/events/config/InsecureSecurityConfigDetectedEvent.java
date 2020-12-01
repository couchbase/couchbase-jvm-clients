/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.events.config;

import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;

/**
 * This event is raised if the SDK sees a potentially insecurely configured security config.
 */
public class InsecureSecurityConfigDetectedEvent extends AbstractEvent {

  private final boolean validateHosts;
  private final boolean insecureTrustManager;

  public InsecureSecurityConfigDetectedEvent(final boolean validateHosts, final boolean insecureTrustManager) {
    super(Severity.WARN, Category.CONFIG, Duration.ZERO, null);

    this.validateHosts = validateHosts;
    this.insecureTrustManager = insecureTrustManager;
  }

  @Override
  public String description() {
    StringBuilder sb = new StringBuilder();
    sb.append("Detected a potentially insecure SecurityConfig - Reason: ");

    if (!validateHosts) {
      sb.append("hostname validation is disabled");
      if (insecureTrustManager) {
        sb.append(" and ");
      }
    }

    if (insecureTrustManager) {
      sb.append("the InsecureTrustManager is used");
    }

    return sb.toString();
  }
}
