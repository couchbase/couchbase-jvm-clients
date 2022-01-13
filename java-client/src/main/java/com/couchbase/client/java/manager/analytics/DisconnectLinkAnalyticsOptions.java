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
 * Customizes how a analytics link is disconnected.
 */
public class DisconnectLinkAnalyticsOptions extends CommonOptions<DisconnectLinkAnalyticsOptions> {

  private Optional<String> linkName = Optional.empty();
  private Optional<String> dataverseName = Optional.empty();

  private DisconnectLinkAnalyticsOptions() {}

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static DisconnectLinkAnalyticsOptions disconnectLinkAnalyticsOptions() {
    return new DisconnectLinkAnalyticsOptions();
  }

  /**
   * Sets the name of the dataverse in which the link should be disconnected.
   *
   * @param dataverseName the name of the dataverse.
   * @return this {@link DisconnectLinkAnalyticsOptions} for chaining purposes.
   */
  public DisconnectLinkAnalyticsOptions dataverseName(final String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
    return this;
  }

  /**
   * Sets the name of the link which should be disconnected.
   *
   * @param linkName the name of the link.
   * @return this {@link DisconnectLinkAnalyticsOptions} for chaining purposes.
   */
  public DisconnectLinkAnalyticsOptions linkName(final String linkName) {
    this.linkName = Optional.ofNullable(linkName);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    Built() { }

    public Optional<String> dataverseName() {
      return dataverseName;
    }

    public Optional<String> linkName() {
      return linkName;
    }
  }
}
