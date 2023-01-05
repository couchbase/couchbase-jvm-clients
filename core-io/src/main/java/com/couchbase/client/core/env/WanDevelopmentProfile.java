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

package com.couchbase.client.core.env;

import com.couchbase.client.core.annotation.Stability;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The "wan-development" environment profile.
 */
@Stability.Volatile
public class WanDevelopmentProfile implements ConfigurationProfile {
  public static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(20);
  public static final Duration KV_TIMEOUT = Duration.ofSeconds(20);
  public static final Duration SERVICE_TIMEOUT = Duration.ofSeconds(120);


  @Override
  public String name() {
    return "wan-development";
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> properties = new HashMap<>();
    String connectTimeout = TimeUnit.MILLISECONDS.toSeconds(CONNECT_TIMEOUT.toMillis()) + "s";
    String kvTimeout = TimeUnit.MILLISECONDS.toSeconds(KV_TIMEOUT.toMillis()) + "s";
    String serviceTimeout = TimeUnit.MILLISECONDS.toSeconds(SERVICE_TIMEOUT.toMillis()) + "s";
    properties.put("timeout.connectTimeout", connectTimeout);
    properties.put("timeout.kvTimeout", kvTimeout);
    properties.put("timeout.kvDurableTimeout", kvTimeout);
    properties.put("timeout.viewTimeout", serviceTimeout);
    properties.put("timeout.queryTimeout", serviceTimeout);
    properties.put("timeout.analyticsTimeout", serviceTimeout);
    properties.put("timeout.searchTimeout", serviceTimeout);
    properties.put("timeout.managementTimeout", serviceTimeout);
    return properties;
  }

}
