/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.endpoint.http;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

class CoreHttpTimeoutHelper {
  private CoreHttpTimeoutHelper() {
    throw new AssertionError("not instantiable");
  }

  private static final Map<ServiceType, Function<TimeoutConfig, Duration>> serviceTypeToDefaultTimeout;

  static {
    Map<ServiceType, Function<TimeoutConfig, Duration>> map = new EnumMap<>(ServiceType.class);
    map.put(ServiceType.ANALYTICS, TimeoutConfig::analyticsTimeout);
    map.put(ServiceType.BACKUP, TimeoutConfig::backupTimeout);
    map.put(ServiceType.EVENTING, TimeoutConfig::eventingTimeout);
    map.put(ServiceType.KV, TimeoutConfig::kvTimeout);
    map.put(ServiceType.MANAGER, TimeoutConfig::managementTimeout);
    map.put(ServiceType.QUERY, TimeoutConfig::queryTimeout);
    map.put(ServiceType.SEARCH, TimeoutConfig::searchTimeout);
    map.put(ServiceType.VIEWS, TimeoutConfig::viewTimeout);
    serviceTypeToDefaultTimeout = unmodifiableMap(map);
  }

  public static Duration resolveTimeout(CoreContext coreContext, ServiceType serviceType, Optional<Duration> timeout) {
    requireNonNull(coreContext);
    requireNonNull(serviceType);
    return timeout.orElseGet(() -> serviceTypeToDefaultTimeout
        .getOrDefault(serviceType, x -> {
          throw new RuntimeException("Can't find default timeout for " + serviceType + " service.");
        })
        .apply(coreContext.environment().timeoutConfig()));
  }
}
