/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public interface PropertyLoader<B extends CoreEnvironment.Builder> {

  void load(B builder);

  /**
   * Returns a new property loader that loads the properties from the given map.
   */
  static <T extends CoreEnvironment.Builder> PropertyLoader<T> fromMap(Map<String, String> properties) {
    Map<String, String> defensiveCopy = unmodifiableMap(new HashMap<>(properties));

    return new AbstractMapPropertyLoader<T>() {
      @Override
      protected Map<String, String> propertyMap() {
        return defensiveCopy;
      }
    };
  }
}
