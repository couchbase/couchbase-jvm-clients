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

package com.couchbase.client.ext.cnc.api;

import com.couchbase.client.core.cnc.Event;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Holds the state for the SDK so it can be consumed by the API.
 *
 * @since 2.0.0
 */
public class State implements Consumer<Event>, DataFetcher {

  private final List<Event> warnings = new ArrayList<>();

  @Override
  public void accept(Event event) {
    if (event.severity().equals(Event.Severity.WARN)) {
      warnings.add(event);
    }
  }

  /**
   * Returns all currently recorded warnings.
   *
   * @return the recorded warnings.
   */
  public List<Map<String, Object>> warnings() {
    List<Map<String, Object>> converted = new ArrayList<>();
    for (Event warning : warnings) {
      Map<String, Object> w = new HashMap<>();
      w.put("name", warning.getClass().getSimpleName());
      w.put("description", warning.description());
      converted.add(w);
    }
    return converted;
  }

  @Override
  public Object get(DataFetchingEnvironment environment) {
    if (environment.getField().getName().equalsIgnoreCase("warnings")) {
      return warnings();
    }
    return null;
  }
}
