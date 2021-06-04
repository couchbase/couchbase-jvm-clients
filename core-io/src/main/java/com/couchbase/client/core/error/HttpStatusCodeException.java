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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpResponseStatus;
import com.couchbase.client.core.error.context.GenericHttpRequestErrorContext;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.CbStrings;

import static com.couchbase.client.core.util.CbThrowables.findCause;

@Stability.Internal
public class HttpStatusCodeException extends CouchbaseException {
  private final int httpStatusCode;
  private final ResponseStatus couchbaseResponseStatus;
  private final String content;

  public HttpStatusCodeException(HttpResponseStatus status, String content, Request<?> request) {
    super("Unexpected HTTP status " + status, new GenericHttpRequestErrorContext(request, status.code()));
    this.httpStatusCode = status.code();
    this.couchbaseResponseStatus = HttpProtocol.decodeStatus(status);
    this.content = CbStrings.nullToEmpty(content);
  }

  /**
   * Returns the Couchbase response status most closely associated with the HTTP status code.
   */
  public ResponseStatus responseStatus() {
    return couchbaseResponseStatus;
  }

  public int httpStatusCode() {
    return httpStatusCode;
  }

  /**
   * HTTP response body as a String. Possibly empty, but never null.
   */
  public String content() {
    return content;
  }

  public static String httpResponseBody(Throwable t) {
    return findCause(t, HttpStatusCodeException.class)
        .map(HttpStatusCodeException::content)
        .orElse("");
  }

  public static ResponseStatus couchbaseResponseStatus(Throwable t) {
    return findCause(t, HttpStatusCodeException.class)
        .map(HttpStatusCodeException::responseStatus)
        .orElse(ResponseStatus.UNKNOWN);
  }

}
