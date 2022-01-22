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

import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * The body of a POST or PUT request.
 * <p>
 * Create an instance using one of the {@link #form} or {@link #json}
 * static factory methods, depending on the desired content type.
 */
public class HttpBody {
  private static final String JSON_CONTENT_TYPE = "application/json";
  private static final String FORM_CONTENT_TYPE = "application/x-www-form-urlencoded";

  final String contentType;
  final byte[] content;

  private HttpBody(String contentType, byte[] content) {
    this.contentType = requireNonNull(contentType);
    this.content = requireNonNull(content);
  }

  /**
   * Creates an HTTP body with content type "application/json" and the given content.
   */
  public static HttpBody json(byte[] json) {
    return new HttpBody(JSON_CONTENT_TYPE, json);
  }

  /**
   * Creates an HTTP body with content type "application/json" and the given content.
   */
  public static HttpBody json(String json) {
    return json(json.getBytes(UTF_8));
  }

  /**
   * Creates an HTTP body with content type "application/x-www-form-urlencoded" and the given form data.
   * <p>
   * This method URL-encodes the names and values; make sure the names and values you pass in are *NOT* already URL-encoded.
   */
  public static HttpBody form(Map<String, ?> data) {
    return form(NameValuePairs.of(data));
  }

  /**
   * Creates an HTTP body with content type "application/x-www-form-urlencoded" and the given form data.
   * <p>
   * This method URL-encodes the names and values; make sure the names and values you pass in are *NOT* already URL-encoded.
   */
  public static HttpBody form(List<NameValuePair> data) {
    return form(NameValuePairs.of(data));
  }

  /**
   * Creates an HTTP body with content type "application/x-www-form-urlencoded" and the given form data.
   * <p>
   * This method URL-encodes the names and values; make sure the names and values you pass in are *NOT* already URL-encoded.
   */
  public static HttpBody form(NameValuePair... data) {
    return form(NameValuePairs.of(data));
  }

  /**
   * Creates an HTTP body with content type "application/x-www-form-urlencoded" and the given form data.
   * <p>
   * This method URL-encodes the names and values; make sure the names and values you pass in are *NOT* already URL-encoded.
   */
  public static HttpBody form(NameValuePairs data) {
    return new HttpBody(FORM_CONTENT_TYPE, data.urlEncoded.getBytes(UTF_8));
  }
}
