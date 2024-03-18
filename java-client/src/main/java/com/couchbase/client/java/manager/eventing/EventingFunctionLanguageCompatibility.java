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
package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.annotation.Stability;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Defines the function language compatibility level.
 */
public enum EventingFunctionLanguageCompatibility {
  /**
   * Uses Server 6.0.0 language compat level.
   */
  VERSION_6_0_0("6.0.0"),

  /**
   * Uses Server 6.5.0 language compat level.
   */
  VERSION_6_5_0("6.5.0"),

  /**
   * Uses Server 6.6.2 language compat level.
   */
  VERSION_6_6_2("6.6.2"),

  /**
   * Uses Server 7.2.0 language compat level.
   */
  VERSION_7_2_0("7.2.0"),
  ;

  private final String value;

  private static final Map<String, EventingFunctionLanguageCompatibility> valueMap = new HashMap<>();

  static {
    for (EventingFunctionLanguageCompatibility v : values()) {
      valueMap.put(v.value, v);
    }
  }

  @Stability.Internal
  @Nullable
  static EventingFunctionLanguageCompatibility parse(@Nullable String s) {
    return valueMap.get(s);
  }

  EventingFunctionLanguageCompatibility(String value) {
    this.value = requireNonNull(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
