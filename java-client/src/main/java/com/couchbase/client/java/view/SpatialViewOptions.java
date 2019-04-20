/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.view;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonArray;

public class SpatialViewOptions extends CommonOptions<SpatialViewOptions> {
  public static SpatialViewOptions DEFAULT = new SpatialViewOptions();

  /**
   * Contains all stored params.
   */
  private final UrlQueryStringBuilder params = UrlQueryStringBuilder.createForUrlSafeNames();
  private boolean development;

  public static SpatialViewOptions spatialViewOptions() {
    return new SpatialViewOptions();
  }

  private SpatialViewOptions() {
  }

  public SpatialViewOptions development(boolean development) {
    this.development = development;
    return this;
  }


  /**
   * Limit the number of the returned documents to the specified number.
   *
   * @param limit the number of documents to return.
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions limit(final int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("Limit must be >= 0.");
    }
    params.set("limit", limit);
    return this;
  }

  /**
   * Skip this number of records before starting to return the results.
   *
   * @param skip The number of records to skip.
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions skip(final int skip) {
    if (skip < 0) {
      throw new IllegalArgumentException("Skip must be >= 0.");
    }
    params.set("skip", skip);
    return this;
  }

  /**
   * Allow the results from a stale view to be used.
   * <p>
   * See the "Stale" enum for more information on the possible options. The
   * default setting is "update_after"!
   *
   * @param stale Which stale mode should be used.
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions stale(final Stale stale) {
    params.set("stale", stale.identifier());
    return this;
  }

  /**
   * Enabled debugging on view queries.
   *
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions debug(boolean debug) {
    params.set("debug", debug);
    return this;
  }

  public SpatialViewOptions startRange(final JsonArray startRange) {
    params.set("start_range", startRange.toString());
    return this;
  }

  public SpatialViewOptions endRange(final JsonArray endRange) {
    params.set("end_range", endRange.toString());
    return this;
  }

  public SpatialViewOptions range(final JsonArray startRange, final JsonArray endRange) {
    startRange(startRange);
    endRange(endRange);
    return this;
  }

  /**
   * Sets the response in the event of an error.
   * <p>
   * See the "OnError" enum for more details on the available options.
   *
   * @param onError The appropriate error handling type.
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions onError(final OnError onError) {
    params.set("on_error", onError.identifier());
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ViewQuery{");
    sb.append("params=\"").append(export()).append('"');
    if (development) {
      sb.append(", dev");
    }
    sb.append('}');
    return sb.toString();
  }


  String export() {
    return params.build();
  }

  @Stability.Internal
  public SpatialViewOptions.BuiltSpatialViewOptions build() {
    return new BuiltSpatialViewOptions();
  }

  public class BuiltSpatialViewOptions extends BuiltCommonOptions {

    public boolean development() {
      return development;
    }

    public String query() {
      return export();
    }

  }

}
