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

import com.couchbase.client.core.annotation.Stability;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HttpGetOptions extends CommonHttpOptions<HttpGetOptions> {

  public static HttpGetOptions httpGetOptions() {
    return new HttpGetOptions();
  }

  private HttpGetOptions() {
  }

  private NameValuePairs queryString = null;

  public HttpGetOptions queryString(Map<String, ?> values) {
    this.queryString = NameValuePairs.of(values);
    return this;
  }

  public HttpGetOptions queryString(List<NameValuePair> values) {
    this.queryString = NameValuePairs.of(values);
    return this;
  }

  public HttpGetOptions queryString(NameValuePair... values) {
    return queryString(Arrays.asList(values));
  }

  public HttpGetOptions preEncodedQueryString(String queryString) {
    this.queryString = NameValuePairs.ofPreEncoded(queryString);
    return this;
  }

  @Stability.Internal
  public HttpGetOptions.Built build() {
    return new HttpGetOptions.Built();
  }

  public class Built extends CommonHttpOptions<HttpGetOptions>.BuiltCommonHttpOptions {
    NameValuePairs queryString() {
      return queryString;
    }
  }
}
