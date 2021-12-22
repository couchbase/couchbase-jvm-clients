/*
 * Copyright (c) 2018 Couchbase, Inc.
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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpResponse;
import com.couchbase.client.core.error.HttpStatusCodeException;
import com.couchbase.client.core.io.netty.HttpChannelContext;

public interface NonChunkedHttpRequest<R extends Response> extends Encodable<FullHttpRequest>, Request<R> {

  R decode(FullHttpResponse response, HttpChannelContext context);

  /**
   * If true, a non-2xx HTTP status code must be reported as an {@link HttpStatusCodeException}.
   * This lets users see the raw HTTP response when making their own HTTP requests with CouchbaseHttpClient.
   * <p>
   * If false, the message handler may throw a domain-specific exception instead.
   */
  default boolean bypassExceptionTranslation() {
    return false;
  }
}
