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

public class DisconnectLinkAnalyticsOptions extends CommonOptions<DisconnectLinkAnalyticsOptions> {
  private Optional<String> linkName = Optional.empty();
  private Optional<String> dataverseName = Optional.empty();

  private DisconnectLinkAnalyticsOptions() {
  }

  public static DisconnectLinkAnalyticsOptions disconnectLinkAnalyticsOptions() {
    return new DisconnectLinkAnalyticsOptions();
  }

  public DisconnectLinkAnalyticsOptions dataverseName(String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
    return this;
  }

  public DisconnectLinkAnalyticsOptions linkName(String linkName) {
    this.linkName = Optional.ofNullable(linkName);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    public Optional<String> dataverseName() {
      return dataverseName;
    }

    public Optional<String> linkName() {
      return linkName;
    }
  }
}
