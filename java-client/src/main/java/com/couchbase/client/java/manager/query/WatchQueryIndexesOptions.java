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

package com.couchbase.client.java.manager.query;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.IndexNotFoundException;

public class WatchQueryIndexesOptions {

  private boolean watchPrimary;

  private WatchQueryIndexesOptions() {
  }

  public static WatchQueryIndexesOptions watchQueryIndexesOptions() {
    return new WatchQueryIndexesOptions();
  }

  /**
   * Include the bucket's primary index in the watch result.
   * If the bucket has no primary index, the watch operation
   * will fail with {@link IndexNotFoundException}.
   */
  public WatchQueryIndexesOptions watchPrimary(boolean watch) {
    this.watchPrimary = watch;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built {
    Built() { }
    public boolean watchPrimary() {
      return watchPrimary;
    }
  }
}
