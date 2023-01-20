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
import com.couchbase.client.java.CommonCreateOptions;

import java.util.Optional;

/**
 * Allows customizing how datasets are created.
 */
public class CreateDatasetAnalyticsOptions extends CommonCreateOptions<CreateDatasetAnalyticsOptions> {

  private Optional<String> dataverseName = Optional.empty();
  private Optional<String> condition = Optional.empty();

  private CreateDatasetAnalyticsOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static CreateDatasetAnalyticsOptions createDatasetAnalyticsOptions() {
    return new CreateDatasetAnalyticsOptions();
  }

  /**
   * Customizes the dataverse from which this dataset should be created.
   *
   * @param dataverseName the name of the dataverse.
   * @return this options class for chaining purposes.
   */
  public CreateDatasetAnalyticsOptions dataverseName(final String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
    return this;
  }

  /**
   * Customizes the "WHERE" clause of the index creation statement.
   *
   * @param condition the condition that should be appended to the where clause.
   * @return this options class for chaining purposes.
   */
  public CreateDatasetAnalyticsOptions condition(final String condition) {
    this.condition = Optional.ofNullable(condition);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCreateOptions {

    Built() { }

    public Optional<String> dataverseName() {
      return dataverseName;
    }

    public Optional<String> condition() {
      return condition;
    }
  }
}
