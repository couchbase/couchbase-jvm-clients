/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.*;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.msg.BaseRequest;
import com.couchbase.client.core.msg.NonChunkedHttpRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;

import static com.couchbase.client.core.io.netty.HttpProtocol.addHttpBasicAuth;

public class UpsertSearchIndexRequest extends BaseRequest<UpsertSearchIndexResponse>
  implements NonChunkedHttpRequest<UpsertSearchIndexResponse> {

  private static final String PATH = "/api/index/";

  private final String name;
  private final Credentials credentials;
  private final byte[] payload;

  public UpsertSearchIndexRequest(Duration timeout, CoreContext ctx, RetryStrategy retryStrategy,
                                  Credentials credentials, String name, byte[] payload) {
    super(timeout, ctx, retryStrategy);
    this.name = name;
    this.credentials = credentials;
    this.payload = payload;
  }

  @Override
  public FullHttpRequest encode() {
    FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, PATH + name,
      Unpooled.wrappedBuffer(payload));
    addHttpBasicAuth(request, credentials.usernameForBucket(""), credentials.passwordForBucket(""));
    request.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    request.headers().set(HttpHeaderNames.CONTENT_LENGTH, payload.length);
    return request;
  }

  @Override
  public UpsertSearchIndexResponse decode(final FullHttpResponse response) {
    byte[] dst = new byte[response.content().readableBytes()];
    response.content().readBytes(dst);
    return new UpsertSearchIndexResponse(HttpProtocol.decodeStatus(response.status()), dst);
  }

  @Override
  public ServiceType serviceType() {
    return ServiceType.SEARCH;
  }
}
