/*
 * Copyright (c) 2019 Couchbase, Inc.
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
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;

import java.net.URLEncoder;

public class ViewOptions extends CommonOptions<ViewOptions> {
  public static ViewOptions DEFAULT = new ViewOptions();

  private static final int PARAM_REDUCE_OFFSET = 0;
  private static final int PARAM_LIMIT_OFFSET = 2;
  private static final int PARAM_SKIP_OFFSET = 4;
  private static final int PARAM_STALE_OFFSET = 6;
  private static final int PARAM_GROUPLEVEL_OFFSET = 8;
  private static final int PARAM_GROUP_OFFSET = 10;
  private static final int PARAM_ONERROR_OFFSET = 12;
  private static final int PARAM_DEBUG_OFFSET = 14;
  private static final int PARAM_DESCENDING_OFFSET = 16;
  private static final int PARAM_INCLUSIVEEND_OFFSET = 18;
  private static final int PARAM_STARTKEY_OFFSET = 20;
  private static final int PARAM_STARTKEYDOCID_OFFSET = 22;
  private static final int PARAM_ENDKEY_OFFSET = 24;
  private static final int PARAM_ENDKEYDOCID_OFFSET = 26;
  private static final int PARAM_KEY_OFFSET = 28;

  /**
   * Number of supported possible params for a query.
   */
  private static final int NUM_PARAMS = 15;

  /**
   * Contains all stored params.
   */
  private final String[] params;

  private boolean development;
  private String keysJson;

  public static ViewOptions viewOptions() {
    return new ViewOptions();
  }

  private ViewOptions() {
    params = new String[NUM_PARAMS * 2];
  }

  public ViewOptions development(boolean development) {
    this.development = development;
    return this;
  }

  /**
   * Explicitly enable/disable the reduce function on the query.
   *
   * @param reduce if reduce should be enabled or not.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions reduce(final boolean reduce) {
    params[PARAM_REDUCE_OFFSET] = "reduce";
    params[PARAM_REDUCE_OFFSET+1] = Boolean.toString(reduce);
    return this;
  }


  /**
   * Limit the number of the returned documents to the specified number.
   *
   * @param limit the number of documents to return.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions limit(final int limit) {
    if (limit < 0) {
      throw new IllegalArgumentException("Limit must be >= 0.");
    }
    params[PARAM_LIMIT_OFFSET] = "limit";
    params[PARAM_LIMIT_OFFSET+1] = Integer.toString(limit);
    return this;
  }

  /**
   * Group the results using the reduce function to a group or single row.
   *
   * Important: this setter and {@link #groupLevel(int)} should not be used
   * together in the same {@link ViewOptions}. It is sufficient to only set the
   * grouping level only and use this setter in cases where you always want the
   * highest group level implictly.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions group(boolean group) {
    params[PARAM_GROUP_OFFSET] = "group";
    params[PARAM_GROUP_OFFSET+1] = Boolean.toString(group);
    return this;
  }

  /**
   * Specify the group level to be used.
   *
   * Important: {@link #group(boolean)} and this setter should not be used
   * together in the same {@link ViewOptions}. It is sufficient to only use this
   * setter and use {@link #group(boolean)} in cases where you always want
   * the highest group level implicitly.
   *
   * @param grouplevel How deep the grouping level should be.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions groupLevel(final int grouplevel) {
    params[PARAM_GROUPLEVEL_OFFSET] = "group_level";
    params[PARAM_GROUPLEVEL_OFFSET+1] = Integer.toString(grouplevel);
    return this;
  }


  /**
   * Specifies whether the specified end key should be included in the result.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions inclusiveEnd(boolean inclusive) {
    params[PARAM_INCLUSIVEEND_OFFSET] = "inclusive_end";
    params[PARAM_INCLUSIVEEND_OFFSET+1] = Boolean.toString(inclusive);
    return this;
  }


  /**
   * Skip this number of records before starting to return the results.
   *
   * @param skip The number of records to skip.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions skip(final int skip) {
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
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions stale(final Stale stale) {
    params[PARAM_STALE_OFFSET] = "stale";
    params[PARAM_STALE_OFFSET+1] = stale.identifier();
    return this;
  }

  /**
   * Sets the response in the event of an error.
   *
   * See the "OnError" enum for more details on the available options.
   *
   * @param onError The appropriate error handling type.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions onError(final OnError onError) {
    params[PARAM_ONERROR_OFFSET] = "on_error";
    params[PARAM_ONERROR_OFFSET+1] = onError.identifier();
    return this;
  }

  /**
   * Enable debugging on view queries.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions debug(boolean debug) {
    params[PARAM_DEBUG_OFFSET] = "debug";
    params[PARAM_DEBUG_OFFSET+1] = Boolean.toString(debug);
    return this;
  }

  /**
   * Return the documents in descending key order.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions descending(boolean desc) {
    params[PARAM_DESCENDING_OFFSET] = "descending";
    params[PARAM_DESCENDING_OFFSET+1] = Boolean.toString(desc);
    return this;
  }


  public ViewOptions key(String key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = encode("\"" + key + "\"");
    return this;
  }

  public ViewOptions key(int key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = Integer.toString(key);
    return this;
  }

  public ViewOptions key(long key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = Long.toString(key);
    return this;
  }

  public ViewOptions key(double key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = Double.toString(key);
    return this;
  }


  public ViewOptions key(boolean key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = Boolean.toString(key);
    return this;
  }

  public ViewOptions key(JsonObject key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = encode(key.toString());
    return this;
  }

  public ViewOptions key(JsonArray key) {
    params[PARAM_KEY_OFFSET] = "key";
    params[PARAM_KEY_OFFSET+1] = encode(key.toString());
    return this;
  }

  public ViewOptions keys(JsonArray keys) {
    this.keysJson = keys.toString();
    return this;
  }

  public ViewOptions startKeyDocId(String id) {
    params[PARAM_STARTKEYDOCID_OFFSET] = "startkey_docid";
    params[PARAM_STARTKEYDOCID_OFFSET+1] = encode(id);
    return this;
  }

  public ViewOptions endKeyDocId(String id) {
    params[PARAM_ENDKEYDOCID_OFFSET] = "endkey_docid";
    params[PARAM_ENDKEYDOCID_OFFSET+1] = encode(id);
    return this;
  }

  public ViewOptions endKey(String key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = encode("\"" + key + "\"");
    return this;
  }

  public ViewOptions endKey(int key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = Integer.toString(key);
    return this;
  }

  public ViewOptions endKey(long key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = Long.toString(key);
    return this;
  }

  public ViewOptions endKey(double key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = Double.toString(key);
    return this;
  }


  public ViewOptions endKey(boolean key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = Boolean.toString(key);
    return this;
  }

  public ViewOptions endKey(JsonObject key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = encode(key.toString());
    return this;
  }

  public ViewOptions endKey(JsonArray key) {
    params[PARAM_ENDKEY_OFFSET] = "endkey";
    params[PARAM_ENDKEY_OFFSET+1] = encode(key.toString());
    return this;
  }

  public ViewOptions startKey(String key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = encode("\"" + key + "\"");
    return this;
  }

  public ViewOptions startKey(int key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = Integer.toString(key);
    return this;
  }

  public ViewOptions startKey(long key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = Long.toString(key);
    return this;
  }

  public ViewOptions startKey(double key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = Double.toString(key);
    return this;
  }

  public ViewOptions startKey(boolean key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = Boolean.toString(key);
    return this;
  }

  public ViewOptions startKey(JsonObject key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = encode(key.toString());
    return this;
  }

  public ViewOptions startKey(JsonArray key) {
    params[PARAM_STARTKEY_OFFSET] = "startkey";
    params[PARAM_STARTKEY_OFFSET+1] = encode(key.toString());
    return this;
  }

  /**
   * Helper method to properly encode a string.
   *
   * This method can be overridden if a different encoding logic needs to be
   * used. If so, note that {@link #keys(JsonArray) keys} is not encoded via
   * this method, but by the core.
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

  /**
   * A string representation of this ViewQuery, suitable for logging and other human consumption.
   * If the {@link #keys(JsonArray)} parameter is too large, it is truncated in this dump.
   *
   * see the {@link #export()} ()} for the parameter representation of the ViewQuery execution URL.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ViewQuery{");
    sb.append("params=\"").append(export()).append('"');
    if (development) {
      sb.append(", dev");
    }
    if (keysJson != null) {
      sb.append(", keys=\"");
      if (keysJson.length() < 140) {
        sb.append(keysJson).append('"');
      } else {
        sb.append(keysJson, 0, 140)
          .append("...\"(")
          .append(keysJson.length())
          .append(" chars total)");
      }
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Returns the query string for this ViewQuery, containing all the key/value pairs
   * for parameters that will be part of this ViewQuery's execution URL for the view service.
   */
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
  public ViewOptions.BuiltViewOptions build() {
    return new ViewOptions.BuiltViewOptions();
  }

  public class BuiltViewOptions extends BuiltCommonOptions {


    public String keys() {
      return keysJson;
    }

    public boolean development() {
      return development;
    }

    public String query() {
      return export();
    }

  }

}
