/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.http;

import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static java.util.Objects.requireNonNull;

/**
 * A name-value pair.
 * <p>
 * Used for specifying repeated query parameters / form properties.
 */
public class NameValuePair {
  final String urlEncoded;

  /**
   * The name and value MUST NOT already be URL-encoded; they will be encoded automatically.
   */
  public NameValuePair(String name, Object value) {
    requireNonNull(name);
    requireNonNull(value);
    this.urlEncoded = urlEncode(name) + "=" + urlEncode(String.valueOf(value));
  }

  /**
   * Static factory method with a concise name suitable for static import.
   *
   * The name and value MUST NOT already be URL-encoded; they will be encoded automatically.
   */
  public static NameValuePair nv(String name, Object value) {
    return new NameValuePair(name, value);
  }

  public String toString() {
    return urlEncoded;
  }
}
