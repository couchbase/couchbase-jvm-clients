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

import java.util.Optional;

/**
 * Customizes how an index is dropped.
 */
public class DropIndexAnalyticsOptions extends CommonOptions<DropIndexAnalyticsOptions> {

  private boolean ignoreIfNotExists;
  private Optional<String> dataverseName = Optional.empty();

  private DropIndexAnalyticsOptions() {}

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static DropIndexAnalyticsOptions dropIndexAnalyticsOptions() {
    return new DropIndexAnalyticsOptions();
  }

  /**
   * Ignore the drop operation if the index does not exist.
   *
   * @param ignore if true no exception will be thrown if the index does not exist.
   * @return this {@link DropIndexAnalyticsOptions} for chaining purposes.
   */
  public DropIndexAnalyticsOptions ignoreIfNotExists(final boolean ignore) {
    this.ignoreIfNotExists = ignore;
    return this;
  }

  /**
   * The name of the dataverse in which the index exists.
   *
   * @param dataverseName the name of the dataverse.
   * @return this {@link DropIndexAnalyticsOptions} for chaining purposes.
   */
  public DropIndexAnalyticsOptions dataverseName(final String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
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

    public Optional<String> dataverseName() {
      return dataverseName;
    }
  }
}
