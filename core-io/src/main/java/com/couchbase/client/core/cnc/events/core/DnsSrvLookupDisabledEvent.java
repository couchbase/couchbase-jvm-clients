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

package com.couchbase.client.core.cnc.events.core;

import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;

/**
 * Raised if dns srv lookup was not performed, either because it has been disabled on the environment or because
 * the hostname list passed in did not qualify.
 */
public class DnsSrvLookupDisabledEvent extends AbstractEvent {

  private final boolean enabledOnEnv;
  private final boolean validDnsSrv;

  public DnsSrvLookupDisabledEvent(final boolean enabledOnEnv, final boolean validDnsSrv) {
    super(Severity.DEBUG, Category.CORE, Duration.ZERO, null);
    this.enabledOnEnv = enabledOnEnv;
    this.validDnsSrv = validDnsSrv;
  }

  @Override
  public String description() {
    return "DNS SRV lookup disabled (enabled on the env: " + enabledOnEnv + ", valid host: " + validDnsSrv + ")";
  }

}
