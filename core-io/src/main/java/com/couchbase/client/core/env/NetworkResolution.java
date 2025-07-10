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

import java.util.Objects;

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Configuration options for the network resolution setting.
 *
 * @since 1.6.0
 */
public class NetworkResolution {

  /**
   * Pick whatever the server returns in the config, this is the
   * old and backwards compatible mode (server default).
   */
  public static final NetworkResolution DEFAULT = new NetworkResolution("default");

  /**
   * Based on heuristics discovers if internal or
   * external resolution will be used.
   * <p>
   * This is the default setting (not to be confused with
   * the default mode)!
   */
  public static final NetworkResolution AUTO = new NetworkResolution("auto");

  /**
   * Pins it to external resolution.
   */
  public static final NetworkResolution EXTERNAL = new NetworkResolution("external");

  /**
   * Stores the internal name.
   */
  private final String name;

  /**
   * Returns a network resolution option with the given name. This allows creating custom
   * values not covered by the statics defined in this class, and also
   * provides a default value of {@link #AUTO} if the given name is empty or null
   * (useful for parsing config properties).
   *
   * @param name the name to use. May be null.
   * @return a {@link NetworkResolution} with the given name, or {@link #AUTO}
   * if the given name is null or empty.
   */
  public static NetworkResolution valueOf(final String name) {
    switch (nullToEmpty(name)) {
      case "":
      case "auto":
        return AUTO;

      case "external":
        return EXTERNAL;

      default:
        return new NetworkResolution(name);
    }
  }

  /**
   * Creates a new network resolution option.
   */
  private NetworkResolution(final String name) {
    this.name = requireNonNull(name);
  }

  /**
   * Returns the wire representation of the network resolution setting.
   */
  public String name() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NetworkResolution that = (NetworkResolution) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return name();
  }
}
