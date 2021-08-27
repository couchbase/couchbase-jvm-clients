/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.annotation.Stability;

import static java.util.Objects.requireNonNull;

/**
 * Prepared statement cache entry.
 */
@Stability.Internal
public class PreparedStatement {
  private final String name;
  private final String encodedPlan;

  /**
   * @param encodedPlan (nullable) null if this is an enhanced prepared statement
   */
  private PreparedStatement(String name, String encodedPlan) {
    this.name = requireNonNull(name);
    this.encodedPlan = encodedPlan;
  }

  public static PreparedStatement enhanced(String name) {
    return new PreparedStatement(name, null);
  }

  public static PreparedStatement legacy(String name, String encodedPlan) {
    return new PreparedStatement(name, requireNonNull(encodedPlan));
  }

  public String name() {
    return name;
  }

  /**
   * @return (nullable) the encoded query plan, or null if this is an enhanced prepared statement
   */
  public String encodedPlan() {
    return encodedPlan;
  }
}
