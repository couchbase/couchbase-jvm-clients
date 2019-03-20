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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.base64.Base64;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.DefaultFullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.FullHttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderNames;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaderValues;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpHeaders;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpRequest;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpVersion;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.HttpProtocol.addHttpBasicAuth;

public class QueryRequest extends BaseRequest<QueryResponse> {

  private static final String URI = "/query";
  private final byte[] query;

  private final Credentials credentials;

  public QueryRequest(final Duration timeout, final CoreContext ctx,
                      final RetryStrategy retryStrategy, final Credentials credentials,
                      final byte[] query) {
    super(timeout, ctx, retryStrategy);
    this.query = query;
    this.credentials = credentials;
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.QUERY;
  }

  public FullHttpRequest encode() {
    ByteBuf content = Unpooled.wrappedBuffer(query);
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
      URI, content);
    request.headers()
      .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    request.headers()
      .set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    request.headers()
      .set(HttpHeaderNames.USER_AGENT, context().environment().userAgent().formattedLong());
    addHttpBasicAuth(request, credentials.usernameForBucket(""), credentials.passwordForBucket(""));
    return request;
  }

}
