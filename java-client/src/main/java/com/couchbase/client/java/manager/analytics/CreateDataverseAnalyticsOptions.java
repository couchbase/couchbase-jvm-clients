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
 * Allows customizing how an analytics dataverse is created.
 */
public class CreateDataverseAnalyticsOptions extends CommonOptions<CreateDataverseAnalyticsOptions> {

  private boolean ignoreIfExists;

  private CreateDataverseAnalyticsOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static CreateDataverseAnalyticsOptions createDataverseAnalyticsOptions() {
    return new CreateDataverseAnalyticsOptions();
  }

  /**
   * Customizes if an already existing dataverse should throw an exception or not (false by default, so it will throw).
   *
   * @param ignore true if no exception should be raised if the dataverse already exists.
   * @return this options class for chaining purposes.
   */
  public CreateDataverseAnalyticsOptions ignoreIfExists(final boolean ignore) {
    this.ignoreIfExists = ignore;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    Built() { }

    public boolean ignoreIfExists() {
      return ignoreIfExists;
    }
  }
}
