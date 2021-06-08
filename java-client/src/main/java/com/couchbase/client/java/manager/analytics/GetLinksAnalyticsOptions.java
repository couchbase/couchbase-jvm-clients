/*
 * Copyright 2021 Couchbase, Inc.
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
import com.couchbase.client.java.manager.analytics.link.AnalyticsLinkType;

import java.util.Optional;

public class GetLinksAnalyticsOptions extends CommonOptions<GetLinksAnalyticsOptions> {
  private Optional<String> dataverseName = Optional.empty();
  private Optional<AnalyticsLinkType> linkType = Optional.empty();
  private Optional<String> name = Optional.empty();

  private GetLinksAnalyticsOptions() {
  }

  public static GetLinksAnalyticsOptions getLinksAnalyticsOptions() {
    return new GetLinksAnalyticsOptions();
  }

  /**
   * Only get links in this dataverse.
   */
  public GetLinksAnalyticsOptions dataverseName(String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
    return this;
  }

  /**
   * Only get the link with this name.
   * <p>
   * Requires specifying {@link #dataverseName(String)}.
   * <p>
   * If there is no link with this name in the specified dataverse,
   * the result is an empty list.
   */
  public GetLinksAnalyticsOptions name(String linkName) {
    this.name = Optional.ofNullable(linkName);
    return this;
  }

  /**
   * Only get links of this type.
   */
  public GetLinksAnalyticsOptions linkType(AnalyticsLinkType linkType) {
    this.linkType = Optional.ofNullable(linkType);
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {
    Built() {
    }

    public Optional<String> dataverseName() {
      return dataverseName;
    }

    public Optional<String> name() {
      return name;
    }

    public Optional<AnalyticsLinkType> linkType() {
      return linkType;
    }
  }
}
