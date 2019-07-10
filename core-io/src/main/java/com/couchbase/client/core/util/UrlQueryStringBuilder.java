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

package com.couchbase.client.core.util;

import com.couchbase.client.core.annotation.Stability;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Stability.Internal
public class UrlQueryStringBuilder {
  private final Map<String, List<String>> params = new LinkedHashMap<>();

  private final boolean encodeNames;

  /**
   * Returns a new instance that assumes none of the parameter names need URL-encoding.
   */
  public static UrlQueryStringBuilder createForUrlSafeNames() {
    return new UrlQueryStringBuilder(false);
  }

  /**
   * Returns a new instance that performs URL-encoding on parameter names as well as values.
   */
  public static UrlQueryStringBuilder create() {
    return new UrlQueryStringBuilder(true);
  }

  private UrlQueryStringBuilder(boolean encodeNames) {
    this.encodeNames = encodeNames;
  }

  private UrlQueryStringBuilder setSafeValue(String name, String value) {
    final List<String> values = new ArrayList<>(1);
    values.add(value);
    params.put(encodeNames ? urlEncode(name) : name, values);
    return this;
  }

  public UrlQueryStringBuilder set(String name, String value) {
    setSafeValue(name, urlEncode(value));
    return this;
  }

  public UrlQueryStringBuilder set(String name, int value) {
    return setSafeValue(name, String.valueOf(value));
  }

  public UrlQueryStringBuilder set(String name, long value) {
    return setSafeValue(name, String.valueOf(value));
  }

  public UrlQueryStringBuilder set(String name, boolean value) {
    return setSafeValue(name, String.valueOf(value));
  }

  private UrlQueryStringBuilder addSafeValue(String name, String value) {
    params.computeIfAbsent(encodeNames ? urlEncode(name) : name, k -> new ArrayList<>()).add(value);
    return this;
  }

  public UrlQueryStringBuilder add(String name, String value) {
    return addSafeValue(name, urlEncode(value));
  }

  public UrlQueryStringBuilder add(String name, int value) {
    return addSafeValue(name, String.valueOf(value));
  }

  public UrlQueryStringBuilder add(String name, long value) {
    return addSafeValue(name, String.valueOf(value));
  }

  public UrlQueryStringBuilder add(String name, boolean value) {
    return addSafeValue(name, String.valueOf(value));
  }

  public String build() {
    final StringBuilder sb = new StringBuilder();

    params.forEach((name, values) -> {
      for (String value : values) {
        if (sb.length() > 0) {
          sb.append("&");
        }
        sb.append(name).append("=").append(value);
      }
    });

    return sb.toString();
  }

  public static String urlEncode(String s) {
    try {
      return URLEncoder.encode(s, StandardCharsets.UTF_8.name())
          .replace("+", "%20"); // Make sure spaces are encoded as "%20"
      // so the result can be used in path components and with "application/x-www-form-urlencoded"
    } catch (UnsupportedEncodingException inconceivable) {
      throw new AssertionError("UTF-8 not supported", inconceivable);
    }
  }

  @Override
  public String toString() {
    return build();
  }
}
