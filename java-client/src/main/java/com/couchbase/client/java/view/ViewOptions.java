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
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import static com.couchbase.client.core.util.Validators.notNull;

public class ViewOptions extends CommonOptions<ViewOptions> {

  /**
   * Contains all stored params.
   */
  private final UrlQueryStringBuilder params = UrlQueryStringBuilder.createForUrlSafeNames();

  private String keysJson;
  private JsonSerializer serializer;
  private DesignDocumentNamespace namespace;

  public static ViewOptions viewOptions() {
    return new ViewOptions();
  }

  private ViewOptions() {
  }

  public ViewOptions namespace(final DesignDocumentNamespace namespace) {
    this.namespace = namespace;
    return this;
  }

  public ViewOptions serializer(final JsonSerializer serializer) {
    this.serializer = serializer;
    return this;
  }

  /**
   * Explicitly enable/disable the reduce function on the query.
   *
   * @param reduce if reduce should be enabled or not.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions reduce(final boolean reduce) {
    params.set("reduce", reduce);
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
      throw InvalidArgumentException.fromMessage("Limit must be >= 0.");
    }
    params.set("limit", limit);
    return this;
  }

  /**
   * Group the results using the reduce function to a group or single row.
   * <p>
   * Important: this setter and {@link #groupLevel(int)} should not be used
   * together in the same {@link ViewOptions}. It is sufficient to only set the
   * grouping level only and use this setter in cases where you always want the
   * highest group level implictly.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions group(boolean group) {
    params.set("group", group);
    return this;
  }

  /**
   * Specify the group level to be used.
   * <p>
   * Important: {@link #group(boolean)} and this setter should not be used
   * together in the same {@link ViewOptions}. It is sufficient to only use this
   * setter and use {@link #group(boolean)} in cases where you always want
   * the highest group level implicitly.
   *
   * @param grouplevel How deep the grouping level should be.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions groupLevel(final int grouplevel) {
    params.set("group_level", grouplevel);
    return this;
  }

  /**
   * Specifies whether the specified end key should be included in the result.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions inclusiveEnd(boolean inclusive) {
    params.set("inclusive_end", inclusive);
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
      throw InvalidArgumentException.fromMessage("Skip must be >= 0.");
    }
    params.set("skip", skip);
    return this;
  }

  /**
   * Sets the scan consistency (staleness) of a view query.
   *
   * @param scanConsistency Which consistency mode should be used.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions scanConsistency(final ViewScanConsistency scanConsistency) {
    params.set("stale", scanConsistency.toString());
    return this;
  }

  /**
   * Sets the response in the event of an error.
   * <p>
   * See the "OnError" enum for more details on the available options.
   *
   * @param viewErrorMode The appropriate error handling type.
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions onError(final ViewErrorMode viewErrorMode) {
    params.set("on_error", viewErrorMode.toString());
    return this;
  }

  /**
   * Enable debugging on view queries.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions debug(boolean debug) {
    params.set("debug", debug);
    return this;
  }

  /**
   * Return the documents in descending key order.
   *
   * @return the {@link ViewOptions} object for proper chaining.
   */
  public ViewOptions order(final ViewOrdering ordering) {
    notNull(ordering, "ViewOrdering");
    params.set("descending", ordering == ViewOrdering.DESCENDING);
    return this;
  }

  public ViewOptions key(String key) {
    params.set("key", "\"" + key + "\"");
    return this;
  }

  public ViewOptions key(int key) {
    params.set("key", key);
    return this;
  }

  public ViewOptions key(long key) {
    params.set("key", key);
    return this;
  }

  public ViewOptions key(double key) {
    params.set("key", Double.toString(key));
    return this;
  }


  public ViewOptions key(boolean key) {
    params.set("key", key);
    return this;
  }

  public ViewOptions key(JsonObject key) {
    params.set("key", key.toString());
    return this;
  }

  public ViewOptions key(JsonArray key) {
    params.set("key", key.toString());
    return this;
  }

  public ViewOptions keys(JsonArray keys) {
    this.keysJson = keys.toString();
    return this;
  }

  public ViewOptions startKeyDocId(String id) {
    params.set("startkey_docid", id);
    return this;
  }

  public ViewOptions endKeyDocId(String id) {
    params.set("endkey_docid", id);
    return this;
  }

  public ViewOptions endKey(String key) {
    params.set("endkey", "\"" + key + "\"");
    return this;
  }

  public ViewOptions endKey(int key) {
    params.set("endkey", key);
    return this;
  }

  public ViewOptions endKey(long key) {
    params.set("endkey", key);
    return this;
  }

  public ViewOptions endKey(double key) {
    params.set("endkey", String.valueOf(key));
    return this;
  }


  public ViewOptions endKey(boolean key) {
    params.set("endkey", key);
    return this;
  }

  public ViewOptions endKey(JsonObject key) {
    params.set("endkey", key.toString());
    return this;
  }

  public ViewOptions endKey(JsonArray key) {
    params.set("endkey", key.toString());
    return this;
  }

  public ViewOptions startKey(String key) {
    params.set("startkey", "\"" + key + "\"");
    return this;
  }

  public ViewOptions startKey(int key) {
    params.set("startkey", key);
    return this;
  }

  public ViewOptions startKey(long key) {
    params.set("startkey", key);
    return this;
  }

  public ViewOptions startKey(double key) {
    params.set("startkey", String.valueOf(key));
    return this;
  }

  public ViewOptions startKey(boolean key) {
    params.set("startkey", key);
    return this;
  }

  public ViewOptions startKey(JsonObject key) {
    params.set("startkey", key.toString());
    return this;
  }

  public ViewOptions startKey(JsonArray key) {
    params.set("startkey", key.toString());
    return this;
  }

  public ViewOptions raw(String key, String value) {
    params.set(key, value);
    return this;
  }

  /**
   * A string representation of this ViewQuery, suitable for logging and other human consumption.
   * If the {@link #keys(JsonArray)} parameter is too large, it is truncated in this dump.
   * <p>
   * see the {@link #export()} ()} for the parameter representation of the ViewQuery execution URL.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ViewQuery{");
    sb.append("params=\"").append(export()).append('"');
    if (namespace == DesignDocumentNamespace.DEVELOPMENT) {
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
    return params.build();
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    Built() { }

    public JsonSerializer serializer() {
      return serializer;
    }

    public String keys() {
      return keysJson;
    }

    public boolean development() {
      return namespace == DesignDocumentNamespace.DEVELOPMENT;
    }

    public String query() {
      return export();
    }

  }

}
