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
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.HttpStatusCodeException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * HTTP status code and response content.
 */
public class HttpResponse {
  private final int statusCode;
  private final byte[] content;

  @Stability.Internal
  public HttpResponse(int statusCode, byte[] content) {
    this.statusCode = statusCode;
    this.content = requireNonNull(content);
  }

  HttpResponse(CoreHttpResponse coreResponse) {
    this(coreResponse.httpStatus(), coreResponse.content());
  }

  HttpResponse(HttpStatusCodeException e) {
    this(e.httpStatusCode(), e.content().getBytes(UTF_8));
  }

  /**
   * Returns the HTTP status code returned by Couchbase Server.
   */
  public int statusCode() {
    return statusCode;
  }

  /**
   * Returns the content of the HTTP response.
   *
   * @return a non-null (but maybe empty) byte array.
   */
  public byte[] content() {
    return content;
  }

  /**
   * Returns the content of the HTTP response interpreted as a UTF-8 string.
   */
  public String contentAsString() {
    return new String(content, UTF_8);
  }

  /**
   * Returns true if the status code is 2xx, otherwise false.
   */
  public boolean success() {
    return statusCode >= 200 && statusCode <= 299;
  }

  @Override
  public String toString() {
    return "HttpResponse{" +
        "statusCode=" + statusCode +
        ", contentAsString=" + contentAsString() +
        '}';
  }
}
