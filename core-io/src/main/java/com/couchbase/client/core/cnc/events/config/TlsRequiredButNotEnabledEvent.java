/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.AbstractEvent;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class TlsRequiredButNotEnabledEvent extends AbstractEvent {
  private final String description;

  private TlsRequiredButNotEnabledEvent(String description) {
    super(Severity.WARN, Category.CONFIG, Duration.ZERO, null);

    this.description = requireNonNull(description);
  }

  public static TlsRequiredButNotEnabledEvent forSharedEnvironment() {
    return new TlsRequiredButNotEnabledEvent(
        "TLS is required when connecting to Couchbase Capella, but is not enabled." +
            " Please enable TLS by setting the 'security.enableTLS' client setting to true."
    );
  }

  public static TlsRequiredButNotEnabledEvent forOwnedEnvironment() {
    return new TlsRequiredButNotEnabledEvent(
        "TLS is required when connecting to Couchbase Capella, but is not enabled." +
            " Please enable TLS by prefixing the connection string with \"couchbases://\" (note the final 's')" +
            " or setting the 'security.enableTLS' client setting to true."
    );
  }

  @Override
  public String description() {
    return description;
  }
}
