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
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonArray;

import java.net.URLEncoder;

public class SpatialViewOptions extends CommonOptions<SpatialViewOptions> {
  public static SpatialViewOptions DEFAULT = new SpatialViewOptions();

  private static final int PARAM_LIMIT_OFFSET = 0;
  private static final int PARAM_SKIP_OFFSET = 2;
  private static final int PARAM_STALE_OFFSET = 4;
  private static final int PARAM_DEBUG_OFFSET = 6;
  private static final int PARAM_START_RANGE_OFFSET = 8;
  private static final int PARAM_END_RANGE_OFFSET = 10;
  private static final int PARAM_ONERROR_OFFSET = 12;

  /**
   * Number of supported possible params for a query.
   */
  private static final int NUM_PARAMS = 7;

  /**
   * Contains all stored params.
   */
  private final String[] params;
  private boolean development;

  public static SpatialViewOptions spatialViewOptions() {
    return new SpatialViewOptions();
  }

  private SpatialViewOptions() {
    params = new String[NUM_PARAMS * 2];
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
    params[PARAM_LIMIT_OFFSET] = "limit";
    params[PARAM_LIMIT_OFFSET+1] = Integer.toString(limit);
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
    params[PARAM_SKIP_OFFSET] = "skip";
    params[PARAM_SKIP_OFFSET+1] = Integer.toString(skip);
    return this;
  }

  /**
   * Allow the results from a stale view to be used.
   *
   * See the "Stale" enum for more information on the possible options. The
   * default setting is "update_after"!
   *
   * @param stale Which stale mode should be used.
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions stale(final Stale stale) {
    params[PARAM_STALE_OFFSET] = "stale";
    params[PARAM_STALE_OFFSET+1] = stale.identifier();
    return this;
  }

  /**
   * Enabled debugging on view queries.
   *
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions debug(boolean debug) {
    params[PARAM_DEBUG_OFFSET] = "debug";
    params[PARAM_DEBUG_OFFSET+1] = Boolean.toString(debug);
    return this;
  }

  public SpatialViewOptions startRange(final JsonArray startRange) {
    params[PARAM_START_RANGE_OFFSET] = "start_range";
    params[PARAM_START_RANGE_OFFSET+1] = startRange.toString();
    return this;
  }

  public SpatialViewOptions endRange(final JsonArray endRange) {
    params[PARAM_END_RANGE_OFFSET] = "end_range";
    params[PARAM_END_RANGE_OFFSET+1] = endRange.toString();
    return this;
  }

  public SpatialViewOptions range(final JsonArray startRange, final JsonArray endRange) {
    startRange(startRange);
    endRange(endRange);
    return this;
  }

  /**
   * Sets the response in the event of an error.
   *
   * See the "OnError" enum for more details on the available options.
   *
   * @param onError The appropriate error handling type.
   * @return the {@link SpatialViewOptions} object for proper chaining.
   */
  public SpatialViewOptions onError(final OnError onError) {
    params[PARAM_ONERROR_OFFSET] = "on_error";
    params[PARAM_ONERROR_OFFSET+1] = onError.identifier();
    return this;
  }

  /**
   * Helper method to properly encode a string.
   *
   * This method can be overridden if a different encoding logic needs to be
   * used.
   *
   * @param source source string.
   * @return encoded target string.
   */
  protected String encode(final String source) {
    try {
      return URLEncoder.encode(source, "UTF-8");
    } catch(Exception ex) {
      throw new RuntimeException("Could not prepare view argument: " + ex);
    }
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
    StringBuilder sb = new StringBuilder();
    boolean firstParam = true;
    for (int i = 0; i < params.length; i++) {
      if (params[i] == null) {
        i++;
        continue;
      }

      boolean even = i % 2 == 0;
      if (even) {
        if (!firstParam) {
          sb.append("&");
        }
      }
      sb.append(params[i]);
      firstParam = false;
      if (even) {
        sb.append('=');
      }
    }
    return sb.toString();
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
