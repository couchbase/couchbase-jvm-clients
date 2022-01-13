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
 * Customizes how a analytics link is connected.
 */
public class ConnectLinkAnalyticsOptions extends CommonOptions<ConnectLinkAnalyticsOptions> {

  private Optional<String> linkName = Optional.empty();
  private Optional<String> dataverseName = Optional.empty();
  private boolean force = false;

  private ConnectLinkAnalyticsOptions() {}

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static ConnectLinkAnalyticsOptions connectLinkAnalyticsOptions() {
    return new ConnectLinkAnalyticsOptions();
  }

  /**
   * Sets the name of the dataverse in which the link should be connected.
   *
   * @param dataverseName the name of the dataverse.
   * @return this {@link ConnectLinkAnalyticsOptions} for chaining purposes.
   */
  public ConnectLinkAnalyticsOptions dataverseName(final String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
    return this;
  }

  /**
   * Sets the name of the link which should be connected.
   *
   * @param linkName the name of the link.
   * @return this {@link ConnectLinkAnalyticsOptions} for chaining purposes.
   */
  public ConnectLinkAnalyticsOptions linkName(final String linkName) {
    this.linkName = Optional.ofNullable(linkName);
    return this;
  }

  /**
   * Customizes if connect link should be forced or not.
   * <p>
   * Determines the behavior of CONNECT LINK if there has been a change in the bucket’s UUID, i.e. the bucket has
   * been deleted and recreated with the same name.
   * <p>
   * <ul>
   *   <li>If force is false, then CONNECT LINK fails. This is the default behavior.</li>
   *   <li>If force is true, CONNECT LINK proceeds: Analytics deletes all existing data in the dataset and ingests
   *   all data from the bucket again.</li>
   * </ul>
   */
  public ConnectLinkAnalyticsOptions force(final boolean force) {
    this.force = force;
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

    public boolean force() {
      return force;
    }
  }
}
