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

package com.couchbase.client.java.manager.view;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static java.util.Objects.requireNonNull;

/**
 * A view index specification consisting of a 'map' function and optionally a 'reduce' function.
 */
@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class View {
  private final String map;
  private final Optional<String> reduce; // nullable

  /**
   * Create a new view with the given map function and no reduce function.
   *
   * @param map map function for this view
   */
  public View(String map) {
    this(map, null);
  }

  /**
   * Creates a new view with a map function and a reduce function.
   *
   * @param map map function for this view
   * @param reduce reduce function for this view (may be null)
   */
  @JsonCreator
  public View(@JsonProperty("map") String map,
              @JsonProperty("reduce") String reduce) {
    this.map = requireNonNull(map);
    this.reduce = Optional.ofNullable(emptyToNull(reduce));
  }

  /**
   * Returns the map function.
   */
  public String map() {
    return map;
  }

  /**
   * Returns the reduce function if this view has one.
   */
  public Optional<String> reduce() {
    return reduce;
  }

  @Override
  public String toString() {
    return "View{" +
        "map='" + redactMeta(map) + '\'' +
        reduce.map(s -> ", reduce='" + redactMeta(s) + '\'').orElse("") +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    View view = (View) o;
    return map.equals(view.map) &&
        Objects.equals(reduce, view.reduce);
  }

  @Override
  public int hashCode() {
    return Objects.hash(map, reduce);
  }
}
