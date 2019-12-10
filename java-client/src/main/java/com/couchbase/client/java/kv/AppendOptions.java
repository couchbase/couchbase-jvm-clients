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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

/**
 * Modifies properties of the append operation.
 */
public class AppendOptions extends CommonDurabilityOptions<AppendOptions> {

  /**
   * The default CAS used (0 means no cas in this context).
   */
  private long cas = 0;

  /**
   * Creates a new {@link AppendOptions}.
   *
   * @return the created options.
   */
  public static AppendOptions appendOptions() {
    return new AppendOptions();
  }

  private AppendOptions() { }

  /**
   * Set the CAS from a previous read operation to perform optimistic concurrency.
   *
   * @param cas the CAS to use for this operation.
   * @return this options class for chaining purposes.
   */
  public AppendOptions cas(final long cas) {
    this.cas = cas;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonDurabilityOptions {

    public long cas() {
      return cas;
    }

  }

}
