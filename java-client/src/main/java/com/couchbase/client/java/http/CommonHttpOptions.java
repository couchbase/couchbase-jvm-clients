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

import com.couchbase.client.java.CommonOptions;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Options common to all HTTP requests.
 */
public class CommonHttpOptions<SELF extends CommonHttpOptions<SELF>> extends CommonOptions<SELF> {

  static class Header {
    final String name;
    final String value;

    Header(String name, String value) {
      this.name = notNull(name, "header name");
      this.value = notNull(value, "header value");
    }
  }

  private final List<Header> headers = new ArrayList<>(0);

  /**
   * Specifies a header to include in the request.
   * May be called multiple times to specify multiple headers.
   * <p>
   * Only a few niche requests require setting a header;
   * most of the time you can ignore this option.
   * <p>
   * NOTE: The "Content-Type" header is set automatically based on the {@link HttpBody}.
   */
  public SELF header(String name, String value) {
    headers.add(new Header(name, value));
    return self();
  }

  public abstract class BuiltCommonHttpOptions extends BuiltCommonOptions {
    public List<Header> headers() {
      return headers;
    }
  }
}
