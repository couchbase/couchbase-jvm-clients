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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.manager.analytics.link.AnalyticsLinkType;

import java.util.Optional;

/**
 * Allows customizing how the analytics links are loaded.
 */
public class GetLinksAnalyticsOptions extends CommonOptions<GetLinksAnalyticsOptions> {

  private Optional<String> dataverseName = Optional.empty();
  private Optional<AnalyticsLinkType> linkType = Optional.empty();
  private Optional<String> name = Optional.empty();

  private GetLinksAnalyticsOptions() {
  }

  /**
   * Creates a new instance with default values.
   *
   * @return the instantiated default options.
   */
  public static GetLinksAnalyticsOptions getLinksAnalyticsOptions() {
    return new GetLinksAnalyticsOptions();
  }

  /**
   * Limits the loading only to the specified dataverse.
   *
   * @param dataverseName the name of the dataverse for which the links should be loaded.
   * @return this options class for chaining purposes.
   */
  public GetLinksAnalyticsOptions dataverseName(final String dataverseName) {
    this.dataverseName = Optional.ofNullable(dataverseName);
    return this;
  }

  /**
   * Limits the loading only to the specified name of the link.
   * <p>
   * If this option is set, the {@link #dataverseName(String)} must also be set.
   *
   * @param linkName the name of the link that should be loaded.
   * @return this options class for chaining purposes.
   */
  public GetLinksAnalyticsOptions name(final String linkName) {
    this.name = Optional.ofNullable(linkName);
    return this;
  }

  /**
   * Limits the loading to only the specified {@link AnalyticsLinkType}.
   *
   * @param linkType the type of link that should be loaded.
   * @return this options class for chaining purposes.
   */
  public GetLinksAnalyticsOptions linkType(final AnalyticsLinkType linkType) {
    this.linkType = Optional.ofNullable(linkType);
    return this;
  }

  @Stability.Internal
  public Built build() {
    if (name.isPresent() && !dataverseName.isPresent()) {
      throw InvalidArgumentException.fromMessage("If a linkName is provided, a dataverseName must also be provided");
    }
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
