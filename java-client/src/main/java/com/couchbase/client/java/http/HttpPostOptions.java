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

public class HttpPostOptions extends CommonHttpOptions<HttpPostOptions> {

  public static HttpPostOptions httpPostOptions() {
    return new HttpPostOptions();
  }

  private HttpPostOptions() {
  }

  private HttpBody body = null;

  public HttpPostOptions body(HttpBody body) {
    this.body = body;
    return this;
  }

  @Stability.Internal
  public HttpPostOptions.Built build() {
    return new HttpPostOptions.Built();
  }

  public class Built extends CommonHttpOptions<HttpPostOptions>.BuiltCommonHttpOptions {
    public HttpBody body() {
      return body;
    }
  }
}
