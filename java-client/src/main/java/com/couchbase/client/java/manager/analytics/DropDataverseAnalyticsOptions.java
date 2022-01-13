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

package com.couchbase.client.java.manager.analytics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.CommonOptions;

/**
 * Allows customizing how an analytics dataverse is dropped.
 */
public class DropDataverseAnalyticsOptions extends CommonOptions<DropDataverseAnalyticsOptions> {

  private boolean ignoreIfNotExists;

  private DropDataverseAnalyticsOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static DropDataverseAnalyticsOptions dropDataverseAnalyticsOptions() {
    return new DropDataverseAnalyticsOptions();
  }

  /**
   * Customizes if a non-existing dataverse should throw an exception or not (false by default, so it will throw).
   *
   * @param ignore true if no exception should be raised if the dataverse does not exist.
   * @return this options class for chaining purposes.
   */
  public DropDataverseAnalyticsOptions ignoreIfNotExists(final boolean ignore) {
    this.ignoreIfNotExists = ignore;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {
    Built() { }
    public boolean ignoreIfNotExists() {
      return ignoreIfNotExists;
    }
  }
}
