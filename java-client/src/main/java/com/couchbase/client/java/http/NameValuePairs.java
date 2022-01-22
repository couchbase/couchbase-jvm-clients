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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.couchbase.client.java.http.NameValuePair.nv;
import static java.util.Objects.requireNonNull;

/**
 * Represents a query string or form data.
 * <p>
 * Create an instance using one of the static factory methods.
 */
public class NameValuePairs {
  final String urlEncoded;

  private NameValuePairs(String urlEncoded) {
    this.urlEncoded = requireNonNull(urlEncoded);
  }

  /**
   * Returns a new instance using the entries of the given map in the map's iteration order.
   *
   * @param map Entries MUST NOT be URL-encoded; they will be URL-encoded automatically
   */
  public static NameValuePairs of(Map<String, ?> map) {
    List<NameValuePair> pairs = new ArrayList<>();
    map.forEach((key, value) -> pairs.add(nv(key, value)));
    return of(pairs);
  }

  /**
   * Returns a new instance that may have repeated names (for example, "number=1&number=2").
   */
  public static NameValuePairs of(NameValuePair... pairs) {
    return of(Arrays.asList(pairs));
  }

  /**
   * Returns a new instance that may have repeated names (for example, "number=1&number=2").
   */
  public static NameValuePairs of(List<NameValuePair> pairs) {
    return new NameValuePairs(
        pairs.stream()
            .map(pair -> pair.urlEncoded)
            .collect(Collectors.joining("&"))
    );
  }

  /**
   * Returns an instance that uses the given string as-is.
   *
   * @param preEncoded URL-encoded query string (without the "?" prefix).
   */
  public static NameValuePairs ofPreEncoded(String preEncoded) {
    return new NameValuePairs(preEncoded);
  }

  @Override
  public String toString() {
    return urlEncoded;
  }
}
