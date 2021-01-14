/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.core.env;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SystemPropertyPropertyLoader extends AbstractMapPropertyLoader<CoreEnvironment.Builder<?>> {

  private static final String PREFIX = "com.couchbase.env.";

  private final Properties properties;

  public SystemPropertyPropertyLoader() {
    this(System.getProperties());
  }

  public SystemPropertyPropertyLoader(final Properties properties) {
    this.properties = properties;
  }

  @Override
  protected Map<String, String> propertyMap() {
    return properties
      .entrySet()
      .stream()
      .filter(entry -> entry.getKey() instanceof String && entry.getValue() instanceof String)
      .filter(entry -> ((String) entry.getKey()).startsWith(PREFIX))
      .collect(Collectors.toMap(e -> ((String) e.getKey()).substring(PREFIX.length()), e -> (String) e.getValue()));
  }

}
