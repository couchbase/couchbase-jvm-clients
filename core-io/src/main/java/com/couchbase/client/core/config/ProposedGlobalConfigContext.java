/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.config;

import com.couchbase.client.core.deps.io.netty.util.internal.ObjectUtil;

import java.util.Objects;

/**
 * This context keeps together a bunch of related information needed to turn a raw
 * config into a parsed one.
 *
 * @author Michael Nitschinger
 * @since 2.0.0
 */
public class ProposedGlobalConfigContext {

  private final String config;
  private final String origin;

  /**
   * Creates a new proposed bucket config context.
   *
   * @param config the raw config, must not be null.
   * @param origin the origin of the config, can be null.
   */
  public ProposedGlobalConfigContext(final String config, final String origin) {
    ObjectUtil.checkNotNull(config, "the raw config cannot be null!");
    this.config = config.replace("$HOST", origin);
    this.origin = origin;
  }

  public String config() {
    return config;
  }

  /**
   * Returns the origin, might be null.
   *
   * @return the origin if set, null otherwise.
   */
  public String origin() {
    return origin;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ProposedGlobalConfigContext that = (ProposedGlobalConfigContext) o;
    return Objects.equals(config, that.config) &&
      Objects.equals(origin, that.origin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, origin);
  }

  @Override
  public String toString() {
    return "ProposedGlobalConfigContext{" +
      "config='" + config + '\'' +
      ", origin='" + origin + '\'' +
      '}';
  }
}